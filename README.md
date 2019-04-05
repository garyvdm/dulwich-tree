This library provides objects to make and commit modifications to tree using dulwich. It try's to provide and interface
similar to making changes to a os filesystem.

### Example:

        writer = TreeWriter(repo)

        writer.set_data('a/b', 'file b'.encode())
        writer.do_commit(message='Add a/b.'.encode())

        writer.set_data('a/b', b'file b ver 2',)
        writer.do_commit(message='Modify a/b.'.encode())

        writer.remove('a/b')
        writer.do_commit(message='Remove a.'.encode())
