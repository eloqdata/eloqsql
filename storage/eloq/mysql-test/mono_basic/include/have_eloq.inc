if (`SELECT COUNT(*) = 0 FROM INFORMATION_SCHEMA.ENGINES WHERE engine = 'eloq' AND support IN ('YES', 'DEFAULT', 'ENABLED')`)
{
  --skip Test requires ha_eloq engine
}
