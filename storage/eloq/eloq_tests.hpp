#pragma once

#ifdef USE_PRAGMA_INTERFACE
#pragma interface /* gcc class implementation */
#endif

#include "ha_eloq.h"

static const MysqlTableSchema *FetchTableSchema(MyEloqTx *my_tx,
                                                const TableName &table_name)
{
  const MysqlTableSchema *session_schema= my_tx->GetTableSchema(table_name);
  if (session_schema == nullptr)
  {
    CatalogKey table_key(table_name);
    CatalogRecord catalog_rec;

    ReadTxRequest &read_req= my_tx->read_req_;
    read_req.Reset();
    read_req.Set(&catalog_ccm_name, &table_key, &catalog_rec, false, false,
                 true);

    TransactionExecution *txm= my_tx->Txm();

    txm->Execute(&read_req);
    read_req.Wait();
    const RecordStatus &rec_status= read_req.Result();

    if (rec_status == RecordStatus::Normal)
    {
      if (catalog_rec.Schema() != nullptr)
      {
        session_schema=
            static_cast<const MysqlTableSchema *>(catalog_rec.Schema());
        my_tx->UpdateTableSchema(table_name, session_schema);
      }
    }
  }

  return session_schema;
}

/**
 * @param test_args formatted as "table1;key-col1,key-col2,key-col3;".
 *  eg. "./db1/table1;1,1,1;"
 */
static int archives_clean_test(const std::string &test_args,
                               MyEloqTx *my_tx, bool bRemove)
{
  if (bRemove)
  {
    return 0;
  }
  std::string table_name_str;
  size_t pos1= test_args.find_first_of(';');
  if (storage_hd == nullptr || test_args.length() == 0 ||
      pos1 == std::string::npos)
  {
    return 1;
  }
  table_name_str= test_args.substr(0, pos1);

  // Split columns by ','
  std::vector<std::string> key_cols;
  pos1= pos1 + 1;
  size_t pos2= test_args.find_first_of(',', pos1);

  while (pos2 != std::string::npos)
  {
    key_cols.push_back(test_args.substr(pos1, pos2 - pos1));
    pos1= pos2 + 1;
    pos2= test_args.find_first_of(',', pos1);
  }
  if (pos1 != test_args.length())
  {
    if (test_args.at(test_args.length() - 1) == ';')
    {
      pos2= test_args.length() - 1;
    }
    else
    {
      pos2= test_args.length();
    }
    key_cols.push_back(test_args.substr(pos1, pos2 - pos1));
  }

  // Fetch key schema and encode key_cols to EloqKey

  TableName table_name{table_name_str, TableType::Primary};

  const MysqlTableSchema *tbl_schema= FetchTableSchema(my_tx, table_name);
  if (tbl_schema == nullptr)
  {
    return 2;
  }
  const TABLE_SHARE *table_share= tbl_schema->TableShare();
  // Only support user defined pk
  if (table_share->primary_key == MAX_INDEXES)
  {
    return 3;
  }
  const EloqKeySchema *key_schema=
      static_cast<const EloqKeySchema *>(tbl_schema->KeySchema());
  std::vector<char> key_blobs;
  key_schema->EncodeKey(key_cols, key_blobs);
  const unsigned char *key_buf=
      reinterpret_cast<const unsigned char *>(key_blobs.data());
  EloqKey key(key_buf, key_blobs.size(), key_schema->KeyInfo());

  // Only clean(flush and kickout) archives of the key's ccentry;
  CleanCcEntryForTestTxRequest clean_req(&table_name, &key, true);
  TransactionExecution *txm= my_tx->Txm();
  txm->Execute(&clean_req);
  clean_req.Wait();

  // Check archives from store
  std::vector<VersionTxRecord> archives;
  storage_hd->FetchArchives(table_name, key, archives, 0U);

  if (archives.size() == 0)
  {
    return 4;
  }

  // Check read the key, should return VersionKnown
  txm->SetStartTs(archives[0].commit_ts_ - 1);
  ReadTxRequest &read_req= my_tx->read_req_;

  read_req.Reset();
  EloqRecord eloq_record;
  read_req.Set(&table_name, &key, &eloq_record, false, false, false);
  txm->SetIsolationLevel(IsolationLevel::Snapshot);
  txm->Execute(&read_req);
  read_req.Wait();
  if (read_req.IsError())
  {
    return 5;
  }
  if (read_req.Result() != RecordStatus::VersionUnknown)
  {
    return 6;
  }

  return 0;
}

/**
 * @param test_args formatted as "table1;key-col1,key-col2,key-col3;".
 *  eg. "./db1/table1;1,1,1;"
 */
static int ccentry_clean_test(const std::string &test_args,
                              MyEloqTx *my_tx, bool bRemove)
{
  if (bRemove)
  {
    return 0;
  }
  std::string table_name_str;
  size_t pos1= test_args.find_first_of(';');
  if (storage_hd == nullptr || test_args.length() == 0 ||
      pos1 == std::string::npos)
  {
    return 1;
  }
  table_name_str= test_args.substr(0, pos1);

  // Split columns by ','
  std::vector<std::string> key_cols;
  pos1= pos1 + 1;
  size_t pos2= test_args.find_first_of(',', pos1);

  while (pos2 != std::string::npos)
  {
    key_cols.push_back(test_args.substr(pos1, pos2 - pos1));
    pos1= pos2 + 1;
    pos2= test_args.find_first_of(',', pos1);
  }
  if (pos1 != test_args.length())
  {
    if (test_args.at(test_args.length() - 1) == ';')
    {
      pos2= test_args.length() - 1;
    }
    else
    {
      pos2= test_args.length();
    }
    key_cols.push_back(test_args.substr(pos1, pos2 - pos1));
  }

  // Fetch key schema and encode key_cols to EloqKey

  TableName table_name{table_name_str, TableType::Primary};

  const MysqlTableSchema *tbl_schema= FetchTableSchema(my_tx, table_name);
  if (tbl_schema == nullptr)
  {
    return 2;
  }
  const TABLE_SHARE *table_share= tbl_schema->TableShare();
  // Only support user defined pk
  if (table_share->primary_key == MAX_INDEXES)
  {
    return 3;
  }
  const EloqKeySchema *key_schema=
      static_cast<const EloqKeySchema *>(tbl_schema->KeySchema());
  std::vector<char> key_blobs;
  key_schema->EncodeKey(key_cols, key_blobs);
  const unsigned char *key_buf=
      reinterpret_cast<const unsigned char *>(key_blobs.data());
  EloqKey key(key_buf, key_blobs.size(), key_schema->KeyInfo());

  // Kickout the key's ccentry with flushing all versions;
  CleanCcEntryForTestTxRequest clean_req(&table_name, &key, false);
  TransactionExecution *txm= my_tx->Txm();
  txm->Execute(&clean_req);
  clean_req.Wait();

  // Check archives from store
  std::vector<VersionTxRecord> archives;
  storage_hd->FetchArchives(table_name, key, archives, 0U);

  if (archives.size() == 0)
  {
    return 4;
  }

  // Check ckpt record (latest version) from store
  EloqRecord store_record;
  bool found= false;
  uint64_t version_ts;
  bool tmp_res=
      storage_hd->Read(table_name, key, store_record, found, version_ts,
                       key_schema, tbl_schema->RecordSchema(),
                       tbl_schema->GetKVCatalogInfo(), tbl_schema->Version());
  if (!tmp_res)
  {
    return 5;
  }
  if (version_ts < 1)
  {
    return 6;
  }

  // Check read the key, should return UnKnown
  ReadTxRequest &read_req= my_tx->read_req_;
  read_req.Reset();
  EloqRecord eloq_record;
  read_req.Set(&table_name, &key, &eloq_record, false, false, false);
  txm->SetIsolationLevel(IsolationLevel::Snapshot);
  txm->Execute(&read_req);
  read_req.Wait();
  if (read_req.IsError())
  {
    return 7;
  }
  if (read_req.Result() != RecordStatus::Unknown)
  {
    return 8;
  }

  return 0;
}

static void split_key_cols(const std::string &key_str,
                           std::vector<std::string> &key_cols,
                           const char sep= ',')
{
  // Split columns
  size_t pos1= 0;
  size_t pos2= key_str.find_first_of(sep, pos1);

  while (pos2 != std::string::npos)
  {
    key_cols.push_back(key_str.substr(pos1, pos2 - pos1));
    pos1= pos2 + 1;
    pos2= key_str.find_first_of(sep, pos1);
  }
  if (pos1 <= key_str.length())
  {
    key_cols.push_back(key_str.substr(pos1, key_str.length() - pos1));
  }
}

/**
 * @param test_args formatted as
 * "table1;sk-name;sk-col1,sk-col2,sk-col3;pk-col1,pk-col2,pk-col3;".
 * eg. "./db1/table1;sk-ab;a,b;1;"
 */
static int sk_archives_clean_test(const std::string &test_args,
                                  MyEloqTx *my_tx, bool bRemove)
{
  if (bRemove)
  {
    return 0;
  }

  // table_name,sk_name,sk_keystr,pk_keystr
  std::vector<std::string> args_vect;
  split_key_cols(test_args, args_vect, ';');
  assert(args_vect.size() >= 4);
  std::string table_name_str= args_vect[0];
  std::string sk_name= args_vect[1];
  std::string sk_keystr= args_vect[2];
  std::string pk_keystr= args_vect[3];

  // Split key cols
  std::vector<std::string> sk_key_cols;
  split_key_cols(sk_keystr, sk_key_cols, ',');

  std::vector<std::string> pk_key_cols;
  split_key_cols(pk_keystr, pk_key_cols, ',');

  // Fetch key schema and encode key_cols to SecondaryKey
  TableName table_name{table_name_str, TableType::Primary};

  const MysqlTableSchema *tbl_schema= FetchTableSchema(my_tx, table_name);
  if (tbl_schema == nullptr)
  {
    return 2;
  }

  std::string index_name_str(table_name_str);
  index_name_str.append(txservice::INDEX_NAME_PREFIX);
  index_name_str.append(sk_name);

  TableName index_name{index_name_str, TableType::Secondary};

  const txservice::SecondaryKeySchema *index_schema=
      tbl_schema->IndexKeySchema(index_name);

  const EloqKeySchema *sk_sk_schema=
      static_cast<const EloqKeySchema *>(index_schema->sk_schema_.get());
  std::vector<char> sk_key_blobs;
  sk_sk_schema->EncodeKey(sk_key_cols, sk_key_blobs);
  const unsigned char *key_buf=
      reinterpret_cast<const unsigned char *>(sk_key_blobs.data());
  EloqKey sk_key(key_buf, sk_key_blobs.size(), sk_sk_schema->KeyInfo());

  const EloqKeySchema *sk_pk_schema=
      static_cast<const EloqKeySchema *>(index_schema->pk_schema_.get());
  std::vector<char> pk_key_blobs;
  sk_pk_schema->EncodeKey(pk_key_cols, pk_key_blobs);
  key_buf= reinterpret_cast<const unsigned char *>(pk_key_blobs.data());
  EloqKey pk_key(key_buf, pk_key_blobs.size(), sk_pk_schema->KeyInfo());

  SecondaryKey<EloqKey, EloqKey> key{sk_key, pk_key};

  // Only clean(flush and kickout) archives of the key's ccentry;
  CleanCcEntryForTestTxRequest clean_req(&index_name, &key, true);
  TransactionExecution *txm= my_tx->Txm();
  txm->Execute(&clean_req);
  clean_req.Wait();

  // Check archives from store
  std::vector<VersionTxRecord> archives;
  storage_hd->FetchArchives(index_name, key, archives, 0U);

  if (archives.size() == 0)
  {
    return 4;
  }

  // Check read the key, should return VersionKnown
  txm->SetStartTs(archives[0].commit_ts_ - 1);
  ReadTxRequest &read_req= my_tx->read_req_;

  read_req.Reset();
  EloqRecord eloq_record;
  read_req.Set(&index_name, &key, &eloq_record, false, false, false);
  txm->SetIsolationLevel(IsolationLevel::Snapshot);
  txm->Execute(&read_req);
  read_req.Wait();
  if (read_req.IsError())
  {
    return 5;
  }
  if (read_req.Result() != RecordStatus::VersionUnknown)
  {
    return 6;
  }

  return 0;
}

/**
 * @param test_args formatted as
 * "table1;sk-name;sk-col1,sk-col2,sk-col3;pk-col1,pk-col2,pk-col3;".
 * eg. "./db1/table1;sk-ab;a,b;1;"
 */
static int sk_ccentry_clean_test(const std::string &test_args,
                                 MyEloqTx *my_tx, bool bRemove)
{
  if (bRemove)
  {
    return 0;
  }

  // table_name,sk_name,sk_keystr,pk_keystr
  std::vector<std::string> args_vect;
  split_key_cols(test_args, args_vect, ';');
  assert(args_vect.size() >= 4);
  std::string table_name_str= args_vect[0];
  std::string sk_name= args_vect[1];
  std::string sk_keystr= args_vect[2];
  std::string pk_keystr= args_vect[3];

  // Split key cols
  std::vector<std::string> sk_key_cols;
  split_key_cols(sk_keystr, sk_key_cols, ',');

  std::vector<std::string> pk_key_cols;
  split_key_cols(pk_keystr, pk_key_cols, ',');

  // Fetch key schema and encode key_cols to SecondaryKey
  TableName table_name{table_name_str, TableType::Primary};

  const MysqlTableSchema *tbl_schema= FetchTableSchema(my_tx, table_name);
  if (tbl_schema == nullptr)
  {
    return 2;
  }

  std::string index_name_str(table_name_str);
  index_name_str.append(txservice::INDEX_NAME_PREFIX);
  index_name_str.append(sk_name);
  TableName index_name{index_name_str, TableType::Secondary};

  const txservice::SecondaryKeySchema *index_schema=
      tbl_schema->IndexKeySchema(index_name);

  const EloqKeySchema *sk_sk_schema=
      static_cast<const EloqKeySchema *>(index_schema->sk_schema_.get());
  std::vector<char> sk_key_blobs;
  sk_sk_schema->EncodeKey(sk_key_cols, sk_key_blobs);
  const unsigned char *key_buf=
      reinterpret_cast<const unsigned char *>(sk_key_blobs.data());
  EloqKey sk_key(key_buf, sk_key_blobs.size(), sk_sk_schema->KeyInfo());

  const EloqKeySchema *sk_pk_schema=
      static_cast<const EloqKeySchema *>(index_schema->pk_schema_.get());
  std::vector<char> pk_key_blobs;
  sk_pk_schema->EncodeKey(pk_key_cols, pk_key_blobs);
  key_buf= reinterpret_cast<const unsigned char *>(pk_key_blobs.data());
  EloqKey pk_key(key_buf, pk_key_blobs.size(), sk_pk_schema->KeyInfo());

  SecondaryKey<EloqKey, EloqKey> key{sk_key, pk_key};

  // Kickout the key's ccentry with flushing all versions;
  CleanCcEntryForTestTxRequest clean_req(&index_name, &key, false);
  TransactionExecution *txm= my_tx->Txm();
  txm->Execute(&clean_req);
  clean_req.Wait();

  // Check archives from store
  std::vector<VersionTxRecord> archives;
  storage_hd->FetchArchives(index_name, key, archives, 0U);

  if (archives.size() == 0)
  {
    return 4;
  }

  // Check read the key, should return VersionKnown
  txm->SetStartTs(archives[0].commit_ts_ - 1);
  ReadTxRequest &read_req= my_tx->read_req_;

  read_req.Reset();
  EloqRecord eloq_record;
  read_req.Set(&index_name, &key, &eloq_record, false, false, false);
  txm->SetIsolationLevel(IsolationLevel::Snapshot);
  txm->Execute(&read_req);
  read_req.Wait();
  if (read_req.IsError())
  {
    return 5;
  }
  if (read_req.Result() != RecordStatus::Unknown)
  {
    return 6;
  }

  return 0;
}

/**
 * @param test_args formatted as "table1;key-col1,key-col2,key-col3;".
 *  eg. "./db1/table1;1,1,1;"
 */
static int ccentry_clean_without_flush_test(const std::string &test_args,
                                            MyEloqTx *my_tx, bool bRemove)
{
  if (bRemove)
  {
    return 0;
  }
  std::string table_name_str;
  size_t pos1= test_args.find_first_of(';');
  if (storage_hd == nullptr || test_args.length() == 0 ||
      pos1 == std::string::npos)
  {
    return 1;
  }
  table_name_str= test_args.substr(0, pos1);

  // Split columns by ','
  std::vector<std::string> key_cols;
  pos1= pos1 + 1;
  size_t pos2= test_args.find_first_of(',', pos1);

  while (pos2 != std::string::npos)
  {
    key_cols.push_back(test_args.substr(pos1, pos2 - pos1));
    pos1= pos2 + 1;
    pos2= test_args.find_first_of(',', pos1);
  }
  if (pos1 != test_args.length())
  {
    if (test_args.at(test_args.length() - 1) == ';')
    {
      pos2= test_args.length() - 1;
    }
    else
    {
      pos2= test_args.length();
    }
    key_cols.push_back(test_args.substr(pos1, pos2 - pos1));
  }

  // Fetch key schema and encode key_cols to EloqKey
  TableName table_name{table_name_str, TableType::Primary};

  const MysqlTableSchema *tbl_schema= FetchTableSchema(my_tx, table_name);
  if (tbl_schema == nullptr)
  {
    return 2;
  }
  const TABLE_SHARE *table_share= tbl_schema->TableShare();
  // Only support user defined pk
  if (table_share->primary_key == MAX_INDEXES)
  {
    return 3;
  }
  const EloqKeySchema *key_schema=
      static_cast<const EloqKeySchema *>(tbl_schema->KeySchema());
  std::vector<char> key_blobs;
  key_schema->EncodeKey(key_cols, key_blobs);
  const unsigned char *key_buf=
      reinterpret_cast<const unsigned char *>(key_blobs.data());
  EloqKey key(key_buf, key_blobs.size(), key_schema->KeyInfo());

  // Kickout the key's ccentry without flushing any versions;
  CleanCcEntryForTestTxRequest clean_req(&table_name, &key, false, false);
  TransactionExecution *txm= my_tx->Txm();
  txm->Execute(&clean_req);
  clean_req.Wait();

  // Check read the key, should return UnKnown
  ReadTxRequest &read_req= my_tx->read_req_;
  read_req.Reset();
  EloqRecord eloq_record;
  read_req.Set(&table_name, &key, &eloq_record, false, false, false);
  txm->SetIsolationLevel(IsolationLevel::Snapshot);
  txm->Execute(&read_req);
  read_req.Wait();
  if (read_req.IsError())
  {
    return 4;
  }
  if (read_req.Result() != RecordStatus::Unknown)
  {
    return 5;
  }

  return 0;
}

static int run_eloq_test(const std::string &test_func,
                              const std::string &test_args,
                              MyEloqTx *my_tx, bool bRemove)
{
  if (test_func == "archives_clean_test")
  {
    return archives_clean_test(test_args, my_tx, bRemove);
  }
  else if (test_func == "ccentry_clean_test")
  {
    return ccentry_clean_test(test_args, my_tx, bRemove);
  }
  else if (test_func == "sk_archives_clean_test")
  {
    return sk_archives_clean_test(test_args, my_tx, bRemove);
  }
  else if (test_func == "sk_ccentry_clean_test")
  {
    return sk_ccentry_clean_test(test_args, my_tx, bRemove);
  }
  else if (test_func == "ccentry_clean_without_flush_test")
  {
    return ccentry_clean_without_flush_test(test_args, my_tx, bRemove);
  }

  return -1;
}
