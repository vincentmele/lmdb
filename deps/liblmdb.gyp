{
    'targets': [{
        'target_name': 'liblmdb'
      , 'defines': [ 'MDB_DEBUG=0' ]
      , 'type': 'static_library'
      , 'standalone_static_library': 1
      , 'direct_dependent_settings': {
            'include_dirs': [
                'liblmdb'
            ]
        }
      , 'conditions': [
            ['OS == "linux"', {
                'cflags': [
                    '-march=native'
                  , '-O2'
                  , '-Waddress'
                  , '-Wno-unused-but-set-variable'
                  , '-fomit-frame-pointer'
                ]
            }]
        ]
      , 'sources': [
            'liblmdb/mdb.c'
          , 'liblmdb/midl.c'
        ]
      , 'test-sources': [
            'liblmdb/mtest2.c'
          , 'liblmdb/mtest3.c'
          , 'liblmdb/mtest4.c'
          , 'liblmdb/mtest5.c'
          , 'liblmdb/mtest6.c'
          , 'liblmdb/mtest.c'
          , 'liblmdb/sample-mdb.c'
        ]
    }]
}
