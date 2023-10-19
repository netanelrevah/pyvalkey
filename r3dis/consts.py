from enum import Enum, auto


class ACLCategories(Enum):
    keyspace = auto()
    read = auto()
    write = auto()
    set = auto()
    sortedset = auto()
    list = auto()
    hash = auto()
    string = auto()
    bitmap = auto()
    hyperloglog = auto()
    geo = auto()
    stream = auto()
    pubsub = auto()
    admin = auto()
    fast = auto()
    slow = auto()
    blocking = auto()
    dangerous = auto()
    connection = auto()
    transaction = auto()
    scripting = auto()


class Commands(Enum):
    llen = auto()
    smismember = auto()
    xread = auto()
    bitcount = auto()
    geodist = auto()
    exists = auto()
    zrevrank = auto()
    zrangebyscore = auto()
    pfcount = auto()
    hmget = auto()
    sunion = auto()
    keys = auto()
    hexists = auto()
    xpending = auto()
    sismember = auto()
    hscan = auto()
    ObjectFrequency = b"OBJECT|FREQ"
    ObjectEncoding = b"OBJECT|ENCODING"
    ObjectIdleTime = b"OBJECT|IDLETIME"
    ObjectReferenceCount = b"OBJECT|REFCOUNT"
    expiretime = auto()
    dump = auto()
    sintercard = auto()
    srandmember = auto()
    sinter = auto()
    hstrlen = auto()
    zrevrange = auto()
    lpos = auto()
    zrevrangebyscore = auto()
    getbit = auto()
    strlen = auto()
    lolwut = auto()
    xlen = auto()
    xinfo__groups = b"XINFO|GROUPS"
    xinfo__consumers = b"XINFO|CONSUMERS"
    xinfo__stream = b"XINFO|STREAM"
    xrevrange = auto()
    zintercard = auto()
    substr = auto()
    mget = auto()
    zinter = auto()
    zrandmember = auto()
    lcs = auto()
    zscore = auto()
    ttl = auto()
    scan = auto()
    zrank = auto()
    sscan = auto()
    geohash = auto()
    hgetall = auto()
    xrange = auto()
    zunion = auto()
    geopos = auto()
    bitfield_ro = auto()
    zmscore = auto()
    zcard = auto()
    georadiusbymember_ro = auto()
    getrange = auto()
    pttl = auto()
    zrange = auto()
    sdiff = auto()
    hvals = auto()
    lindex = auto()
    georadius_ro = auto()
    dbsize = auto()
    zscan = auto()
    zcount = auto()
    smembers = auto()
    randomkey = auto()
    sort_ro = auto()
    memory__usage = b"MEMORY|USAGE"
    hrandfield = auto()
    hkeys = auto()
    hget = auto()
    zdiff = auto()
    geosearch = auto()
    zrangebylex = auto()
    hlen = auto()
    lrange = auto()
    zrevrangebylex = auto()
    pexpiretime = auto()
    type_ = auto()
    scard = auto()
    touch = auto()
    bitpos = auto()
    zlexcount = auto()
    Acl = b"SERVER_CONTEXT"
    AclCategory = b"SERVER_CONTEXT|CATEGORY"
    AclDeleteUser = b"SERVER_CONTEXT|DELUSER"
    AclDryRun = b"SERVER_CONTEXT|DRYRUN"
    AclGeneratePassword = b"SERVER_CONTEXT|GENPASS"
    AclGetUser = b"SERVER_CONTEXT|GETUSER"
    AclList = b"SERVER_CONTEXT|LIST"
    AclLoad = b"SERVER_CONTEXT|LOAD"
    AclLog = b"SERVER_CONTEXT|LOG"
    AclSave = b"SERVER_CONTEXT|SAVE"
    AclSetUser = b"SERVER_CONTEXT|SETUSER"
    AclUsers = b"SERVER_CONTEXT|USERS"
    AclWhoAmI = b"SERVER_CONTEXT|WHOAMI"
    AclHelp = b"SERVER_CONTEXT|HELP"
    Client = b"CLIENT"
    ClientSetName = b"CLIENT|SETNAME"
    ClientPause = b"CLIENT|PAUSE"
    ClientUnpause = b"CLIENT|UNPAUSE"
    ClientReply = b"CLIENT|REPLY"
    ClientKill = b"CLIENT|KILL"
    ClientGetName = b"CLIENT|GETNAME"
    ClientId = b"CLIENT|ID"
    ClientList = b"CLIENT|LIST"
    ClientSetInformation = b"CLIENT|SETINFO"
    Config = b"CONFIG"
    ConfigSet = b"CONFIG|SET"
    ConfigGet = b"CONFIG|GET"
    Information = b"INFO"
    Authorize = b"AUTH"
    FlushDatabase = b"FLUSHDB"
    Select = b"SELECT"
    Delete = b"DEL"
    Get = b"GET"
    Set = b"SET"
    Increment = b"INCR"
    Decrement = b"DECR"
    IncrementBy = b"INCRBY"
    DecrementBy = b"DECRBY"
    IncrementByFloat = b"INCRBYFLOAT"
    Keys = b"KEYS"
    Append = b"APPEND"
    Ping = b"PING"
    DatabaseSize = b"DBSIZE"
    Echo = b"ECHO"
    GetBit = b"GETBIT"
    SetBit = b"SETBIT"
    BitCount = b"BITCOUNT"
    BitOperation = b"BITOP"
    HashMapGet = b"HGET"
    HashMapValues = b"HVALS"
    HashMapStringLength = b"HSTRLEN"
    HashMapDelete = b"HDEL"
    HashMapSet = b"HSET"
    HashMapGetAll = b"HGETALL"
    HashMapExists = b"HEXISTS"
    HashMapIncreaseBy = b"HINCRBY"
    HashMapIncreaseByFloat = b"HINCRBYFLOAT"
    HashMapKeys = b"HKEYS"
    HashMapLength = b"HLEN"
    HashMapGetMultiple = b"HMGET"
    HashMapSetMultiple = b"HMSET"
    SetPop = b"SPOP"
    SetRemove = b"SREM"
    SetUnion = b"SUNION"
    SetIntersection = b"SINTER"
    SetDifference = b"SDIFF"
    SetUnionStore = b"SUNIONSTORE"
    SetIntersectionStore = b"SINTERSTORE"
    SetDifferenceStore = b"SDIFFSTORE"
    SetAdd = b"SADD"
    SetIsMember = b"SISMEMBER"
    SetMembers = b"SMEMBERS"
    SetAreMembers = b"SMISMEMBER"
    SetMove = b"SMOVE"
    SetCardinality = b"SCARD"
    SortedSetRange = b"ZRANGE"
    SortedSetRangeStore = b"ZRANGESTORE"
    SortedSetReversedRange = b"ZREVRANGE"
    SortedSetRangeByScore = b"ZRANGEBYSCORE"
    SortedSetReversedRangeByScore = b"ZREVRANGEBYSCORE"
    SortedSetRangeByLexical = b"ZRANGEBYLEX"
    SortedSetReversedRangeByLexical = b"ZREVRANGEBYLEX"
    SortedSetCount = b"ZCOUNT"
    SortedSetCardinality = b"ZCARD"
    SortedSetAdd = b"ZADD"
    ListRange = b"LRANGE"
    ListPush = b"LPUSH"
    ListPop = b"LPOP"
    ListRemove = b"LREM"
    ListPushAtTail = b"RPUSH"
    ListLength = b"LLEN"
    ListIndex = b"LINDEX"
    ListInsert = b"LINSERT"


COMMANDS_PER_CATEGORY = {
    ACLCategories.read: [
        Commands.llen,
        Commands.smismember,
        Commands.xread,
        Commands.bitcount,
        Commands.geodist,
        Commands.exists,
        Commands.zrevrank,
        Commands.zrangebyscore,
        Commands.pfcount,
        Commands.hmget,
        Commands.sunion,
        Commands.keys,
        Commands.hexists,
        Commands.xpending,
        Commands.sismember,
        Commands.hscan,
        Commands.Get,
        Commands.ObjectFrequency,
        Commands.ObjectEncoding,
        Commands.ObjectIdleTime,
        Commands.ObjectReferenceCount,
        Commands.expiretime,
        Commands.dump,
        Commands.sintercard,
        Commands.srandmember,
        Commands.sinter,
        Commands.hstrlen,
        Commands.zrevrange,
        Commands.lpos,
        Commands.zrevrangebyscore,
        Commands.getbit,
        Commands.strlen,
        Commands.lolwut,
        Commands.xlen,
        Commands.xinfo__groups,
        Commands.xinfo__consumers,
        Commands.xinfo__stream,
        Commands.xrevrange,
        Commands.zintercard,
        Commands.substr,
        Commands.mget,
        Commands.zinter,
        Commands.zrandmember,
        Commands.lcs,
        Commands.zscore,
        Commands.ttl,
        Commands.scan,
        Commands.zrank,
        Commands.sscan,
        Commands.geohash,
        Commands.hgetall,
        Commands.xrange,
        Commands.zunion,
        Commands.geopos,
        Commands.bitfield_ro,
        Commands.zmscore,
        Commands.zcard,
        Commands.georadiusbymember_ro,
        Commands.getrange,
        Commands.pttl,
        Commands.zrange,
        Commands.sdiff,
        Commands.hvals,
        Commands.lindex,
        Commands.georadius_ro,
        Commands.dbsize,
        Commands.zscan,
        Commands.zcount,
        Commands.smembers,
        Commands.randomkey,
        Commands.sort_ro,
        Commands.memory__usage,
        Commands.hrandfield,
        Commands.hkeys,
        Commands.hget,
        Commands.zdiff,
        Commands.geosearch,
        Commands.zrangebylex,
        Commands.hlen,
        Commands.lrange,
        Commands.zrevrangebylex,
        Commands.pexpiretime,
        Commands.type_,
        Commands.scard,
        Commands.touch,
        Commands.bitpos,
        Commands.zlexcount,
    ]
}
