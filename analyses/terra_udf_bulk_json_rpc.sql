{{if_data_call_function(
        func = "streamline.udf_bulk_json_rpc_sbx_shah(object_construct('sql_source', 'view_name', 'external_table', 'qn_getBlockWithReceipts', 'sql_limit', 4000, 'producer_batch_size', 10000s, 'worker_batch_size', 1000, 'batch_call_limit', 10))",
        target = "streamline.view_name"
    )
}}

SELECT
    streamline.udf_bulk_json_rpc(
        object_construct(
            'sql_source',
            'streamline__pc_getBlock_realtime',
            'external_table',
            'pc_getBlock',
            'sql_limit',
            4000,
            'producer_batch_size',
            10000,
            'worker_batch_size',
            1000,
            'batch_call_limit',
            10
        )
    )