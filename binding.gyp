{
  'targets': [{
    'target_name': 'nlmdb',
    'include_dirs' : [
      '<!(node -p -e "require(\'path\').dirname(require.resolve(\'nan\'))")',
      "/usr/lib/x86_64-linux-gnu",
      "/usr/local/include",
    ],
    'library_dirs' : [
      "/usr/local/lib",
    ],

    "libraries": [
      "liblmdb.a"
    ],
    'sources': [
      'src/nlmdb.cc',
      'src/database.cc',
      'src/database_async.cc',
      'src/batch.cc',
      'src/batch_async.cc',
      'src/iterator.cc',
      'src/iterator_async.cc',
    ]
  }]
}