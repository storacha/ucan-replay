/**
 * @template {any} T
 */
export default class AsyncBuffer {
  /** @type {T[]} */
  _buffer
  /** @type {((any) => void) | null}*/
  _resolve
  /** @type {number} */
  _batchLength
  /** @type {boolean} */
  _closed

  /** @param {number} batchLength  */
  constructor(batchLength) {
    this._batchLength = batchLength
    this._buffer = []
    this._resolve = null
    this._closed = false
  }

  /**
   * 
   * @param {AsyncGenerator<T[]>} itemsStream 
   */
  async streamIn(itemsStream) {
    for await (const items of itemsStream) {
      this._buffer = this._buffer.concat(items)
      if (this._resolve != null) {
        this._resolve()
      }
    }
    this._closed = true
    if (this._resolve != null) {
      this._resolve()
    }
  }

  /**
   * @returns {AsyncGenerator<T[]>}
   */
  async * streamOut() {
    for (;;) {
      if (this._buffer.length >= this._batchLength) {
        const nextItems = this._buffer.slice(0, this._batchLength)
        this._buffer = this._buffer.slice(this._batchLength)
        yield(nextItems)
      } else if (this._closed) {
        if (this._buffer.length > 0) {
          yield(this._buffer)
        }
        return
      } else {
        await new Promise((resolve) => {
          this._resolve = resolve
        })
        this._resolve = null
      }
    }
  }
} 