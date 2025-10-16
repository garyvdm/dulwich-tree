import stat
from typing import Dict, List, Optional, Sequence, Union, cast

from dulwich.errors import NotTreeError
from dulwich.objects import Blob, ObjectID, ShaFile, Tree
from dulwich.objectspec import parse_tree
from dulwich.refs import Ref as DulwichRef
from dulwich.repo import Repo

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
        message: Optional[bytes] = None,
        committer: Optional[bytes] = None,
        author: Optional[bytes] = None,
        commit_timestamp=None,
        commit_timezone=None,
        author_timestamp=None,
        author_timezone=None,
        encoding: Optional[bytes] = None,
        merge_heads: Optional[List[bytes]] = None,
        no_verify: bool = False,
        sign: bool = False,
    ):
        self.add_changed_to_object_store()
        ret = self.repo.do_commit(
            tree=self.tree.id,
            ref=self.ref,
            # From args
            message=message,
            committer=committer,
            author=author,
            commit_timestamp=commit_timestamp,
            commit_timezone=commit_timezone,
            author_timestamp=author_timestamp,
            author_timezone=author_timezone,
            encoding=encoding,
            merge_heads=merge_heads,
            no_verify=no_verify,
            sign=sign,
        )
        self.reset()
        return ret
