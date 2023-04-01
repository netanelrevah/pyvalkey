from enum import StrEnum, auto


class ACLCategories(StrEnum):
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


class Commands(StrEnum):
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
    get = auto()
    object__freq = "object|freq"
    object__encoding = "object|encoding"
    object__idletime = "object|idletime"
    object__refcount = "object|refcount"
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
    xinfo__groups = "xinfo|groups"
    xinfo__consumers = "xinfo|consumers"
    xinfo__stream = "xinfo|stream"
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
    memory__usage = "memory|usage"
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
        Commands.get,
        Commands.object__freq,
        Commands.object__encoding,
        Commands.object__idletime,
        Commands.object__refcount,
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
