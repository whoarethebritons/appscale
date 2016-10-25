@0xe5659228c7bbd12c;

struct AppLog {
  time @0 :Float32;
  level @1 :Int8;
  message @2 :Text;
  sourceLocation @3: Text;
}

struct RequestLog {
  fileOnDisk @0 : Text;
  appId @1 :Text;
  moduleId @2 :Text;
  versionId @3 :Text;
  requestId @4 :Data;
  offset @5 :Data;
  ip @6: Text;
  nickname @7: Text;
  startTime @8: Float64;
  endTime @9: Float64;
  latency @10: Float64;
  mcycles @11: Int32;
  method @12: Text;
  resource @13: Text;
  httpVersion @14: Text;
  status @15: Text;
  responseSize @16: Int32;
  referrer @17: Text;
  userAgent @18: Text;
  urlMapEntry @19: Text;
  combined @20: Text;
  host @21: Text;
  cost @22: Float32;
  taskQueueName @23: Text;
  taskName @24: Text;
  wasLoadingRequest @25: Int8;
  pendingTime @26: Float32;
  replicaIndex @27: Int32;
  finished @28: Int8;
  instanceKey @29: Text;
  appLogs @30: List(AppLog);
  appEngineRelease @31: Text;
}
    
