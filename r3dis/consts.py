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


class Command(Enum):
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
    ObjectFrequency = b"object|freq"
    ObjectEncoding = b"object|encoding"
    ObjectIdleTime = b"object|idletime"
    ObjectReferenceCount = b"object|refcount"
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
    xinfo__groups = b"xinfo|groups"
    xinfo__consumers = b"xinfo|consumers"
    xinfo__stream = b"xinfo|stream"
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
    memory__usage = b"memory|usage"
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
    Acl = b"acl"
    AclCategory = b"acl|category"
    AclDelUser = b"acl|deluser"
    AclDryRun = b"acl|dryrun"
    AclGenPass = b"acl|genpass"
    AclGetUser = b"acl|getuser"
    AclList = b"acl|list"
    AclLoad = b"acl|load"
    AclLog = b"acl|log"
    AclSave = b"acl|save"
    AclSetUser = b"acl|setuser"
    AclUsers = b"acl|users"
    AclWhoAmI = b"acl|whoami"
    AclHelp = b"acl|help"
    Client = b"client"
    ClientSetName = b"client|setname"
    ClientPause = b"client|pause"
    ClientUnpause = b"client|unpause"
    ClientReply = b"client|reply"
    ClientKill = b"client|kill"
    ClientGetName = b"client|getname"
    ClientId = b"client|id"
    ClientList = b"client|list"
    Config = b"config"
    ConfigSet = b"config|set"
    ConfigGet = b"config|get"
    Information = b"info"
    Authorize = b"auth"
    FlushDatabase = b"flushdb"
    Select = b"select"
    Delete = b"del"
    Get = b"get"
    Set = b"set"
    Increment = b"incr"
    Decrement = b"decr"
    IncrementBy = b"incrby"
    DecrementBy = b"decrby"
    IncrementByFloat = b"incrbyfloat"
    Keys = b"keys"
    Append = b"append"
    Ping = b"ping"
    DatabaseSize = b"dbsize"
    Echo = b"echo"
    GetBit = b"getbit"
    SetBit = b"setbit"
    BitCount = b"bitcount"
    BitOperation = b"bitop"
    HashMapGet = b"hget"
    HashMapValues = b"hvals"
    HashMapStringLength = b"hstrlen"
    HashMapDelete = b"hdel"
    HashMapSet = b"hset"
    HashMapGetAll = b"hgetall"
    HashMapExists = b"hexists"
    HashMapIncreaseBy = b"hincrby"
    HashMapIncreaseByFloat = b"hincrbyfloat"
    HashMapKeys = b"hkeys"
    HashMapLength = b"hlen"
    HashMapGetMultiple = b"hmget"
    HashMapSetMultiple = b"hmset"
    SetPop = b"spop"
    SetRemove = b"srem"
    SetUnion = b"sunion"
    SetIntersection = b"sinter"
    SetDifference = b"sdiff"
    SetUnionStore = b"sunionstore"
    SetIntersectionStore = b"sinterstore"
    SetDifferenceStore = b"sdiffstore"
    SetAdd = b"sadd"
    SetIsMember = b"sismember"
    SetMembers = b"smembers"
    SetMultipleIsMember = b"smismember"
    SetMove = b"smove"
    SetCardinality = b"scard"
    SortedSetRange = b"zrange"
    SortedSetRangeStore = b"zrangestore"
    SortedSetReversedRange = b"zrevrange"
    SortedSetRangeByScore = b"zrangebyscore"
    SortedSetReversedRangeByScore = b"zrevrangebyscore"
    SortedSetRangeByLexical = b"zrangebylex"
    SortedSetReversedRangeByLexical = b"zrevrangebylex"
    SortedSetCount = b"zcount"
    SortedSetAdd = b"zadd"
    ListRange = b"lrange"
    ListPush = b"lpush"
    ListPop = b"lpop"
    ListRemove = b"lrem"
    ListPushAtTail = b"rpush"
    ListLength = b"llen"
    ListIndex = b"lindex"
    ListInsert = b"linsert"


COMMANDS_PER_CATEGORY = {
    ACLCategories.read: [
        Command.llen,
        Command.smismember,
        Command.xread,
        Command.bitcount,
        Command.geodist,
        Command.exists,
        Command.zrevrank,
        Command.zrangebyscore,
        Command.pfcount,
        Command.hmget,
        Command.sunion,
        Command.keys,
        Command.hexists,
        Command.xpending,
        Command.sismember,
        Command.hscan,
        Command.Get,
        Command.ObjectFrequency,
        Command.ObjectEncoding,
        Command.ObjectIdleTime,
        Command.ObjectReferenceCount,
        Command.expiretime,
        Command.dump,
        Command.sintercard,
        Command.srandmember,
        Command.sinter,
        Command.hstrlen,
        Command.zrevrange,
        Command.lpos,
        Command.zrevrangebyscore,
        Command.getbit,
        Command.strlen,
        Command.lolwut,
        Command.xlen,
        Command.xinfo__groups,
        Command.xinfo__consumers,
        Command.xinfo__stream,
        Command.xrevrange,
        Command.zintercard,
        Command.substr,
        Command.mget,
        Command.zinter,
        Command.zrandmember,
        Command.lcs,
        Command.zscore,
        Command.ttl,
        Command.scan,
        Command.zrank,
        Command.sscan,
        Command.geohash,
        Command.hgetall,
        Command.xrange,
        Command.zunion,
        Command.geopos,
        Command.bitfield_ro,
        Command.zmscore,
        Command.zcard,
        Command.georadiusbymember_ro,
        Command.getrange,
        Command.pttl,
        Command.zrange,
        Command.sdiff,
        Command.hvals,
        Command.lindex,
        Command.georadius_ro,
        Command.dbsize,
        Command.zscan,
        Command.zcount,
        Command.smembers,
        Command.randomkey,
        Command.sort_ro,
        Command.memory__usage,
        Command.hrandfield,
        Command.hkeys,
        Command.hget,
        Command.zdiff,
        Command.geosearch,
        Command.zrangebylex,
        Command.hlen,
        Command.lrange,
        Command.zrevrangebylex,
        Command.pexpiretime,
        Command.type_,
        Command.scard,
        Command.touch,
        Command.bitpos,
        Command.zlexcount,
    ]
}
