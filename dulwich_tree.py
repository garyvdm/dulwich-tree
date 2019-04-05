import stat

from dulwich.errors import NotTreeError
from dulwich.objects import Blob, Tree
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


class _RefCounted(object):

    __slots__ = ('ref_count', 'obj')

    def __init__(self, obj, ref_count=0):
        self.obj = obj
        self.ref_count = ref_count

    def __repr__(self):
        return "_RefCounted({!r}, ref_count={})".format(self.obj, self.ref_count)

    def inc_ref_count(self):
        self.ref_count += 1

    def dec_ref_count(self):
        self.ref_count -= 1


class TreeWriter(TreeReader):

    def __init__(self, repo, ref=b'HEAD', encoding="UTF-8"):
        self.repo = repo
        self.encoding = encoding
        self.ref = ref
        self.reset()

    def reset(self):
        try:
            self.org_commit_id = self.repo.refs[self.ref]
        except KeyError:
            self.org_commit_id = None
            self.tree = Tree()
        else:
            self.tree = parse_tree(self.repo, self.org_commit_id)
            self.org_tree_id = self.tree.id
        self.changed_objects = {}

    def _add_changed_object(self, obj):
        ref_counted = self.changed_objects.get(obj.id)
        if not ref_counted:
            self.changed_objects[obj.id] = ref_counted = _RefCounted(obj)
        ref_counted.ref_count += 1

    def _remove_changed_object(self, obj_id):
        ref_counted = self.changed_objects.get(obj_id)
        if ref_counted:
            ref_counted.ref_count -= 1
            if ref_counted.ref_count == 0:
                del self.changed_objects[obj_id]

    def lookup_obj(self, sha):
        try:
            return self.changed_objects[sha].obj
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

        for old_tree, name in reversed(tuple(zip(old_trees, path_items))):
            new_tree = old_tree.copy()

            if obj is None or obj.id == EMPTY_TREE_SHA:
                old_obj_id, _ = new_tree[name]
                self._remove_changed_object(old_obj_id)
                del new_tree[name]
                # print(f'del old: {old_tree} new: {new_tree} name: {name}')
            else:
                self._add_changed_object(obj)
                new_tree[name] = (mode, obj.id)
                # print(f'set old: {old_tree} new: {new_tree} name: {name} obj_id: {obj_id}')

            obj = new_tree
            mode = stat.S_IFDIR

        self._remove_changed_object(old_tree)
        self._add_changed_object(obj)
        self.tree = obj

    def set_data(self, path, data, mode=stat.S_IFREG | 0o644):
        obj = Blob()
        obj.data = data
        self.set(path, obj, mode)
        return obj

    def remove(self, path):
        self.set(path, None, None)

    def add_changed_to_object_store(self):
        self.repo.object_store.add_objects([(ref_counted.obj, None) for ref_counted in self.changed_objects.values()])

    def do_commit(self, **kwargs):
        self.add_changed_to_object_store()
        ret = self.repo.do_commit(tree=self.tree.id, ref=self.ref, **kwargs)
        self.reset()
        return ret
