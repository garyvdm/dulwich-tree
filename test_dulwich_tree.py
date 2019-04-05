import os.path
import unittest

import dulwich.tests
from dulwich.errors import NotTreeError
from dulwich.repo import MemoryRepo, Repo

from dulwich_tree import TreeReader, TreeWriter


class TestTreeReader(unittest.TestCase):

    def setUp(self):
        repo = Repo(os.path.join(dulwich.tests.__path__[0], 'data/repos/a.git'))
        self.reader = TreeReader(repo)

    def test_lookup(self):
        self.assertEqual(
            (33188, b'4ef30bbfe26431a69c3820d3a683df54d688f2ec'),
            self.reader.lookup('a')
        )

    def test_get(self):
        self.assertEqual(
            b'file a\n',
            self.reader.get('a').as_raw_string()
        )

    def test_tree_items(self):
        self.assertEqual(
            ['a', 'b', 'c'],
            self.reader.tree_items('/')
        )

    def test_tree_items_not_tree_error(self):
        with self.assertRaises(NotTreeError):
            self.reader.tree_items('a')

    def test_exists(self):
        self.assertTrue(self.reader.exists('a'))
        self.assertFalse(self.reader.exists('non_existing'))


class TestTreeWriter(unittest.TestCase):

    # TODO Test the commit that gets created.

    def test(self):
        # TODO This test needs to be broken up.
        repo = MemoryRepo()
        writer = TreeWriter(repo)
        writer.set_data('a', b'file a',)
        self.assertEqual(b'file a', writer.get('a').data)
        writer.do_commit(message=b'Add a.')
        self.assertEqual(b'file a', writer.get('a').data)
        self.assertEqual(b'file a', TreeReader(repo).get('a').data)

        writer.set_data('b/c', b'file c',)
        writer.do_commit(message=b'Add b/c.')
        self.assertEqual(b'file c', writer.get('b/c').data)

        writer.set_data('b/c', b'file c ver 2',)
        writer.do_commit(message=b'Modify b/c.')
        self.assertEqual(b'file c ver 2', writer.get('b/c').data)

        writer.remove('a')
        writer.do_commit(message=b'Remove a.')
        self.assertFalse(writer.exists('a'))

        writer.remove('b/c')
        writer.do_commit(message=b'Remove b/c.')
        self.assertFalse(writer.exists('b/c'))

    def test_rm_non_existant(self):
        repo = MemoryRepo()
        writer = TreeWriter(repo)
        writer.set_data('a/b', b'b')
        with self.assertRaises(KeyError):
            writer.remove('a/c')

        writer.set_data('a/d', b'd')
