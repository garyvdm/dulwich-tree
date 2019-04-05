import logging
import stat
import subprocess
from contextlib import suppress
from time import time, timezone

from dulwich.errors import NotTreeError
from dulwich.objects import Blob, Commit, Tree
from dulwich.objectspec import parse_tree

EMPTY_TREE_SHA = b'4b825dc642cb6eb9a060e54bf8d69288fbee4904'

class TreeReader(object):

    def __init__(self, repo, treeish='HEAD', encoding="UTF-8"):
        self.repo = repo
        self.treeish = treeish
        self.tree = None
        self.lookup_obj = repo.__getitem__
        self.encoding = encoding
        self.reset()

    def reset(self):
        self.tree = parse_tree(self.repo, self.treeish)

    def lookup(self, path):
        return self.tree.lookup_path(self.lookup_obj, path.encode(self.encoding))

    def get(self, path):
        _, sha = self.tree.lookup_path(self.lookup_obj, path.encode(self.encoding))
        return self.lookup_obj(sha)

    def tree_items(self, path):
        tree = self.get(path)
        if not isinstance(tree, Tree):
            raise NotTreeError(path)
        return [item.decode(self.encoding) for item in tree]

    def exists(self, path):
        try:
            self.lookup(path)
        except KeyError:
            return False
        else:
            return True


class TreeWriter(TreeReader):

    # TODO: changed_objects should have a ref count

    def __init__(self, repo, branch=b'HEAD', encoding="UTF-8"):
        self.repo = repo
        self.encoding = encoding
        self.branch = branch
        self.reset()

    def reset(self):
        try:
            self.org_commit_id = self.repo.refs[self.branch]
        except KeyError:
            self.org_commit_id = None
            self.tree = Tree()
        else:
            self.tree = parse_tree(self.repo, self.org_commit_id)
            self.org_tree_id = self.tree.id
        self.changed_objects = {}

    def lookup_obj(self, sha):
        try:
            return self.changed_objects[sha]
        except KeyError:
            return self.repo[sha]

    def set(self, path, obj, mode):
        path_items = path.encode(self.encoding).split(b'/')
        sub_tree = self.tree
        old_trees = [sub_tree]
        for name in path_items[:-1]:
            try:
                _, sub_tree_sha = sub_tree[name]
            except KeyError:
                sub_tree = Tree()
            else:
                sub_tree = self.lookup_obj(sub_tree_sha)
            old_trees.append(sub_tree)

        new_objs = []
        for old_tree, name in reversed(tuple(zip(old_trees, path_items))):
            new_tree = old_tree.copy()

            if obj is None or obj.id == EMPTY_TREE_SHA:
                if name not in new_tree:
                    raise KeyError(name)
                del new_tree[name]
                # print(f'del old: {old_tree} new: {new_tree} name: {name}')
            else:
                obj_id = obj.id
                new_objs.append(obj)
                new_tree[name] = (mode, obj_id)
                # print(f'set old: {old_tree} new: {new_tree} name: {name} obj_id: {obj_id}')

            obj = new_tree
            mode = stat.S_IFDIR

        new_objs.append(obj)
        self.tree = obj

        # print(f'old: {old_trees} new: {new_objs}')
        for old_tree in old_trees:
            if old_tree:
                with suppress(KeyError):
                    del self.changed_objects[old_tree.id]
        for obj in new_objs:
            self.changed_objects[obj.id] = obj

    def set_data(self, path, data, mode=stat.S_IFREG | 0o644):
        obj = Blob()
        obj.data = data
        self.set(path, obj, mode)
        return obj

    def remove(self, path):
        self.set(path, None, None)

    def commit(self, message, author=None):
        commit = Commit()
        commit.tree = self.tree.id
        if author is None:
            config = self.repo.get_config_stack()
            author = self.repo._get_user_identity(config)
        else:
            author = author.encode(self.encoding)
        commit.author = commit.committer = author
        commit.commit_time = commit.author_time = int(time())
        tz = timezone
        commit.commit_timezone = commit.author_timezone = tz
        commit.message = message.encode(self.encoding)
        commit.encoding = self.encoding.encode('ascii')
        if self.org_commit_id:
            commit.parents = [self.org_commit_id]

        commit_id = commit.id
        self.changed_objects[commit_id] = commit
        self.repo.object_store.add_objects([(obj, None) for obj in self.changed_objects.values()])
        self.repo.refs.set_if_equals(self.branch, self.org_commit_id, commit_id)

        self.reset()
