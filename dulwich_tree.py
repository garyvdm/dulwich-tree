import stat
import time
from typing import Dict, Optional, Sequence, Union, cast

from dulwich.errors import NotTreeError, CommitError
from dulwich.objects import Blob, ObjectID, ShaFile, Tree, Commit
from dulwich.objectspec import parse_tree
from dulwich.refs import Ref as DulwichRef
from dulwich.repo import Repo, get_user_identity, check_user_identity

__all__ = ["Ref", "TreeReader", "TreeWriter"]

Ref = Union[str, DulwichRef]

EMPTY_TREE_SHA = b"4b825dc642cb6eb9a060e54bf8d69288fbee4904"


class TreeReader:
    def __init__(self, repo: Repo, treeish: Ref = "HEAD", encoding: str = "UTF-8"):
        self.repo = repo
        if isinstance(treeish, str):
            treeish = treeish.encode(encoding)
        self.treeish = treeish
        self.lookup_obj = repo.__getitem__
        self.encoding = encoding
        # TODO use config encoding
        # encoding = config.get(("i18n",), "commitEncoding")
        self.reset()

    def reset(self) -> None:
        self.tree: Tree = parse_tree(self.repo, self.treeish)

    def lookup(self, path: str):
        return self.tree.lookup_path(self.lookup_obj, path.encode(self.encoding))

    def get(self, path: str):
        _, sha = self.tree.lookup_path(self.lookup_obj, path.encode(self.encoding))
        return self.lookup_obj(sha)

    def tree_items(self, path: str) -> Sequence[str]:
        tree = self.get(path)
        if not isinstance(tree, Tree):
            raise NotTreeError(path.encode(self.encoding))

        return [item.decode(self.encoding) for item in tree]

    def exists(self, path: str) -> bool:
        try:
            self.lookup(path)
        except KeyError:
            return False
        else:
            return True


class _RefCounted:
    __slots__ = ("ref_count", "obj")

    def __init__(self, obj: ShaFile, ref_count: int = 0):
        self.obj = obj
        self.ref_count = ref_count

    def __repr__(self):
        return "_RefCounted({!r}, ref_count={})".format(self.obj, self.ref_count)


class TreeWriter(TreeReader):
    def __init__(self, repo: Repo, ref: Ref = "HEAD", encoding: str = "UTF-8"):
        self.repo = repo
        self.encoding = encoding
        if isinstance(ref, str):
            ref = ref.encode(encoding)
        self.ref: DulwichRef = ref
        self.reset()

    def reset(self):
        try:
            self.org_commit_id = self.repo.refs[self.ref]
        except KeyError:
            self.org_commit_id = None
            self.tree = Tree()
        else:
            self.tree: "Tree" = parse_tree(self.repo, self.org_commit_id)
            self.org_tree_id = self.tree.id
        self.changed_objects: Dict[ObjectID, _RefCounted] = {}

    def _add_changed_object(self, obj: ShaFile):
        ref_counted = self.changed_objects.get(obj.id)
        if not ref_counted:
            self.changed_objects[obj.id] = ref_counted = _RefCounted(obj)
        ref_counted.ref_count += 1

    def _remove_changed_object(self, obj_id: ObjectID):
        ref_counted = self.changed_objects.get(obj_id)
        if ref_counted:
            ref_counted.ref_count -= 1
            if ref_counted.ref_count == 0:
                del self.changed_objects[obj_id]

    def lookup_obj(self, sha: ObjectID) -> ShaFile:
        try:
            return self.changed_objects[sha].obj
        except KeyError:
            return self.repo[sha]

    def set(self, path: str, obj: Optional[ShaFile], mode: Optional[int]):
        path_items = path.encode(self.encoding).split(b"/")
        sub_tree = self.tree
        old_trees = [sub_tree]
        for name in path_items[:-1]:
            try:
                _, sub_tree_sha = sub_tree[name]
            except KeyError:
                sub_tree = Tree()
            else:
                sub_tree = cast(Tree, self.lookup_obj(sub_tree_sha))
            old_trees.append(sub_tree)

        for old_tree, name in reversed(tuple(zip(old_trees, path_items))):
            new_tree = cast(Tree, old_tree.copy())

            if obj is None or obj.id == EMPTY_TREE_SHA:
                old_obj_id, _ = new_tree[name]
                self._remove_changed_object(old_obj_id)
                del new_tree[name]
            else:
                self._add_changed_object(obj)
                new_tree[name] = (mode, obj.id)

            obj = new_tree
            mode = stat.S_IFDIR

        self._remove_changed_object(old_tree.id)
        self._add_changed_object(new_tree)
        self.tree = new_tree

    def set_data(self, path: str, data: bytes, mode: int = stat.S_IFREG | 0o644):
        obj = Blob()
        obj.data = data
        self.set(path, obj, mode)
        return obj

    def remove(self, path: str):
        self.set(path, None, None)

    def add_changed_to_object_store(self):
        self.repo.object_store.add_objects(
            [(ref_counted.obj, None) for ref_counted in self.changed_objects.values()]
        )

    def do_commit(
        self,
        message: Union[str, bytes],
        committer: Optional[Union[str, bytes]] = None,
        author: Optional[Union[str, bytes]] = None,
        commit_timestamp=None,
        commit_timezone=None,
        author_timestamp=None,
        author_timezone=None,
        sign: bool = False,
    ):
        """Commit changes.

        If not specified, committer and author default to
        get_user_identity(..., 'COMMITTER')
        and get_user_identity(..., 'AUTHOR') respectively.

        Args:
          message: Commit message (bytes or callable that takes (repo, commit)
            and returns bytes)
          committer: Committer fullname
          author: Author fullname
          commit_timestamp: Commit timestamp (defaults to now)
          commit_timezone: Commit timestamp timezone (defaults to GMT)
          author_timestamp: Author timestamp (defaults to commit
            timestamp)
          author_timezone: Author timestamp timezone
            (defaults to commit timestamp timezone)
          sign: GPG Sign the commit (bool, defaults to False,
            pass True to use default GPG key,
            pass a str containing Key ID to use a specific GPG key)

        Returns:
          New commit SHA1
        """
        c = Commit()
        c.tree = self.tree.id

        config = self.repo.get_config_stack()
        if committer is None:
            committer = get_user_identity(config, kind="COMMITTER")
        elif isinstance(committer, str):
            committer = committer.encode(self.encoding)
        check_user_identity(committer)
        c.committer = committer
        if commit_timestamp is None:
            commit_timestamp = time.time()
        c.commit_time = int(commit_timestamp)
        if commit_timezone is None:
            # FIXME: Use current user timezone rather than UTC
            commit_timezone = 0
        c.commit_timezone = commit_timezone
        if author is None:
            author = get_user_identity(config, kind="AUTHOR")
        elif isinstance(author, str):
            author = author.encode(self.encoding)
        c.author = author
        check_user_identity(author)
        if author_timestamp is None:
            author_timestamp = commit_timestamp
        c.author_time = int(author_timestamp)
        if author_timezone is None:
            author_timezone = commit_timezone
        c.author_timezone = author_timezone
        c.encoding = self.encoding.encode()

        try:
            old_head = self.repo.refs[self.ref]
        except KeyError:
            old_head = None

        if old_head:
            c.parents = [old_head]
        else:
            c.parents = []

        if isinstance(message, str):
            message = message.encode(self.encoding)
        c.message = message

        # Check if we should sign the commit
        should_sign = sign
        if sign is None:
            # Check commit.gpgSign configuration when sign is not explicitly set
            try:
                should_sign = config.get_boolean((b"commit",), b"gpgSign")
            except KeyError:
                should_sign = False  # Default to not signing if no config
        keyid = sign if isinstance(sign, str) else None

        if should_sign:
            c.sign(keyid)

        self._add_changed_object(c)
        self.add_changed_to_object_store()

        if old_head:
            ok = self.repo.refs.set_if_equals(
                self.ref,
                old_head,
                c.id,
                message=b"commit: " + c.message,
                committer=c.committer,
                timestamp=c.commit_time,
                timezone=c.commit_timezone,
            )
        else:
            ok = self.repo.refs.add_if_new(
                self.ref,
                c.id,
                message=b"commit: " + c.message,
                committer=c.committer,
                timestamp=c.commit_timezone,
                timezone=c.commit_timezone,
            )

        if not ok:
            # Fail if the atomic compare-and-swap failed, leaving the
            # commit and all its objects as garbage.
            raise CommitError(f"{self.ref!r} changed during commit")

        self.reset()

        return c.id
