/* @flow */

const stream = require('readable-stream')
const varint = require('varint')
const xtend = require('xtend')
const duplexify = require('duplexify')
const log = require('debug')('mplex')

const Channel = require('./channel')
import type {ChannelOpts} from './channel'

const SIGNAL_FLUSH = new Buffer([0])

const empty = new Buffer(0)
let pool = new Buffer(10 * 1024)
let used = 0

type MultiplexOpts = {
  binaryName?: bool,
  limit?: number
}

type ChannelCallback = (Channel) => void

class Multiplex extends stream.Duplex {
  constructor (opts?: MultiplexOpts | ChannelCallback, onchannel?: ChannelCallback) {
    super()
    console.log('MULTIPLEX STARTING')
    if (typeof opts === 'function') {
      onchannel = opts
      opts = {}
    }

    if (!opts) {
      opts = {}
    }

    if (onchannel) {
      this.on('stream', onchannel)
    }

    this.destroyed = false
    this.limit = opts.limit || 0

    this._corked = 0
    this._options = opts
    this._binaryName = !!opts.binaryName
    this._local = []
    this._remote = []
    this._list = this._local
    this._receiving = null
    this._chunked = false
    this._state = 0
    this._type = 0
    this._channel = 0
    this._missing = 0
    this._message = null

    let bufSize = 100
    if (this.limit) {
      bufSize = varint.encodingLength(this.limit)
    }
    this._buf = new Buffer(bufSize)
    this._ptr = 0
    this._awaitChannelDrains = 0
    this._onwritedrain = null
    this._ondrain = []
    this._finished = false

    this.on('finish', this._clear)
  }

  createStream (name: Buffer | string, opts: ChannelOpts): Channel {
    if (this.destroyed) {
      throw new Error('Multiplexer is destroyed')
    }

    let id = this._local.indexOf(null)

    if (id === -1) {
      id = this._local.push(null) - 1
    }
    let channelName = this._name(name || id.toString())
    log('createStream: ' + channelName.toString())

    const channel = new Channel(
      channelName,
      this,
      xtend(this._options, opts)
    )

    return this._addChannel(channel, id, this._local)
  }

  receiveStream (name: Buffer | string, opts: ChannelOpts): Channel {
    if (this.destroyed) {
      throw new Error('Multiplexer is destroyed')
    }

    if (name === undefined || name === null) {
      throw new Error('Name is needed when receiving a stream')
    }

    const channelName = this._name(name)
    log('receiveStream: ' + channelName.toString())
    const channel = new Channel(
      channelName,
      this,
      xtend(this._options, opts)
    )

    if (!this._receiving) {
      this._receiving = {}
    }

    if (this._receiving[channel.name]) {
      throw new Error('You are already receiving this stream')
    }

    this._receiving[channel.name] = channel

    return channel
  }

  createSharedStream (name: Buffer | string, opts: ChannelOpts): stream.Duplex {
    log('createSharedStream')
    return duplexify(this.createStream(name, xtend(opts, {lazy: true})), this.receiveStream(name, opts))
  }

  _name (name: Buffer | string): Buffer | string {
    if (!this._binaryName) {
      return name.toString()
    }
    return Buffer.isBuffer(name) ? name : new Buffer(name)
  }

  _send (header: number, data?: Buffer): bool {
    const len = data ? data.length : 0
    const oldUsed = used
    let drained = true

    log('_send', header, len)

    varint.encode(header, pool, used)
    used += varint.encode.bytes
    varint.encode(len, pool, used)
    used += varint.encode.bytes

    drained = this.push(pool.slice(oldUsed, used))

    if (pool.length - used < 100) {
      pool = new Buffer(10 * 1024)
      used = 0
    }

    if (data) {
      drained = this.push(data)
    }

    return drained
  }

  _addChannel (channel: Channel, id: number, list: Array<Channel|null>): Channel {
    log('_addChannel', id)
    while (list.length <= id) {
      list.push(null)
    }

    list[id] = channel
    channel.on('finalize', () => {
      list[id] = null
    })
    channel.open(id, list === this._local)

    return channel
  }

  _writeVarint (data: Buffer, offset: number): number {
    for (offset; offset < data.length; offset++) {
      if (this._ptr === this._buf.length) {
        return this._lengthError(data)
      }

      this._buf[this._ptr++] = data[offset]

      if (!(data[offset] & 0x80)) {
        if (this._state === 0) {
          const header = varint.decode(this._buf)
          this._type = header & 7
          this._channel = header >> 3
          this._list = this._type & 1 ? this._local : this._remote
          const chunked = this._list.length > this._channel &&
                this._list[this._channel] &&
                this._list[this._channel].chunked

          this._chunked = !!(this._type === 1 || this._type === 2) && chunked
        } else {
          this._missing = varint.decode(this._buf)

          if (this.limit && this._missing > this.limit) {
            return this._lengthError(data)
          }
        }

        this._state++
        this._ptr = 0
        return offset + 1
      }
    }

    return data.length
  }

  _lengthError (data: Buffer): number {
    this.destroy(new Error('Incoming message is too big'))
    return data.length
  }

  _writeMessage (data: Buffer, offset: number): number {
    const free = data.length - offset
    const missing = this._missing

    if (!this._message) {
      if (missing <= free) { // fast track - no copy
        this._missing = 0
        this._push(data.slice(offset, offset + missing))
        return offset + missing
      }
      if (this._chunked) {
        this._missing -= free
        this._push(data.slice(offset, data.length))
        return data.length
      }
      this._message = new Buffer(missing)
    }

    data.copy(this._message, this._ptr, offset, offset + missing)

    if (missing <= free) {
      this._missing = 0
      this._push(this._message)
      return offset + missing
    }

    this._missing -= free
    this._ptr += free

    return data.length
  }

  _push (data: Buffer) {
    log('_push', data.length)
    if (!this._missing) {
      this._ptr = 0
      this._state = 0
      this._message = null
    }

    if (this._type === 0) { // open
      if (this.destroyed || this._finished) {
        return
      }

      let name
      if (this._binaryName) {
        name = data
      } else {
        name = data.toString() || this._channel.toString()
      }

      let channel
      if (this._receiving && this._receiving[name]) {
        channel = this._receiving[name]
        delete this._receiving[name]
        this._addChannel(channel, this._channel, this._list)
      } else {
        channel = new Channel(name, this, this._options)
        this.emit('stream', this._addChannel(
          channel,
          this._channel,
          this._list), channel.name)
      }
      return
    }

    const stream = this._list[this._channel] ||
      this._remote[this._channel] ||
      this._local[this._channel]

    if (!stream) {
      return
    }

    switch (this._type) {
      case 5: // local error
      case 6: // remote error
        const error = new Error(data.toString() || 'Channel destroyed')
        stream._destroy(error, false)
        return

      case 3: // local end
      case 4: // remote end
        stream.push(null)
        return

      case 1: // local packet
      case 2: // remote packet
        if (!stream.push(data)) {
          this._awaitChannelDrains++
          stream._awaitDrain++
        }
        return
    }
  }

  _onchanneldrain (drained: number) {
    this._awaitChannelDrains -= drained

    if (this._awaitChannelDrains) {
      return
    }

    const ondrain = this._onwritedrain
    this._onwritedrain = null

    if (ondrain) {
      ondrain()
    }
  }

  _write (data: Buffer, enc: string, cb: () => void) {
    log('_write', data.length)
    if (this._finished) {
      cb()
      return
    }

    if (this._corked) {
      this._onuncork(this._write.bind(this, data, enc, cb))
      return
    }

    if (data === SIGNAL_FLUSH) {
      this._finish(cb)
      return
    }

    let offset = 0
    while (offset < data.length) {
      if (this._state === 2) {
        offset = this._writeMessage(data, offset)
      } else {
        offset = this._writeVarint(data, offset)
      }
    }

    if (this._state === 2 && !this._missing) {
      this._push(empty)
    }

    if (this._awaitChannelDrains) {
      this._onwritedrain = cb
    } else {
      cb()
    }
  }

  _finish (cb: () => void) {
    this._onuncork(() => {
      if (this._writableState.prefinished === false) {
        this._writableState.prefinished = true
      }
      this.emit('prefinish')
      this._onuncork(cb)
    })
  }

  cork () {
    if (++this._corked === 1) {
      this.emit('cork')
    }
  }

  uncork () {
    if (this._corked && --this._corked === 0) {
      this.emit('uncork')
    }
  }

  end (data?: Buffer | () => void, enc?: string | () => void, cb?: () => void) {
    log('end')
    if (typeof data === 'function') {
      cb = data
      data = undefined
    }
    if (typeof enc === 'function') {
      cb = enc
      enc = undefined
    }

    if (data) {
      this.write(data)
    }

    if (!this._writableState.ending) {
      this.write(SIGNAL_FLUSH)
    }

    return stream.Writable.prototype.end.call(this, cb)
  }

  _onuncork (fn: () => void) {
    if (this._corked) {
      this.once('uncork', fn)
      return
    }

    fn()
  }

  _read () {
    while (this._ondrain.length) {
      this._ondrain.shift()()
    }
  }

  _clear () {
    if (this._finished) {
      return
    }

    this._finished = true

    const list = this._local.concat(this._remote)

    this._local = []
    this._remote = []

    list.forEach(function (stream) {
      if (stream) {
        stream._destroy(null, false)
      }
    })

    this.push(null)
  }

  finalize () {
    this._clear()
  }

  destroy (err?: Error) {
    log('destroy')
    if (this.destroyed) {
      log('already destroyed')
      return
    }

    var list = this._local.concat(this._remote)

    this.destroyed = true

    if (err) {
      this.emit('error', err)
    }
    this.emit('close')

    list.forEach(function (stream) {
      if (stream) {
        stream.emit('error', err || new Error('underlying socket has been closed'))
      }
    })

    this._clear()
  }
}

module.exports = Multiplex
