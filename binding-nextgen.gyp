{
    'targets': [{
        'target_name': 'nlmdb'
      , 'include_dirs' : [
          '<!(node -p -e "require(\'path\').dirname(require.resolve(\'nan\'))")'
        ],
        "libraries": [
          "/usr/lib/x86_64-linux-gnu/liblmdb.a" 
        ],
     'sources': [
            'src/nlmdb.cc'
          , 'src/database.cc'
          , 'src/database_async.cc'
          , 'src/batch.cc'
          , 'src/batch_async.cc'
          , 'src/iterator.cc'
          , 'src/iterator_async.cc'
        ]
    }]
}