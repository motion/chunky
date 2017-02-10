import FS from 'fs'
import ES from 'event-stream'

/*
   Takes a file path, reads it as a stream, calls back for each chunk,
     returns a promise that resolves once finished with all chunks.
*/

export default ({ file, chunk = 5000, limit }, onChunk) => {
  return new Promise((respond, reject) => {
    if (!file) {
      throw new Error(`No file given to chunky`)
    }

    let total = 0
    let chunkIndex = 0
    let rows = []

    const callback = async (piece, resume) => {
      const result = await Promise.resolve(onChunk(piece, chunkIndex))
      chunkIndex += 1
      resume(result)
    }

    const $ = FS.createReadStream(file)
      .pipe(ES.split())
      .pipe(ES.mapSync(async line => {
        total += 1

        if (total > 0 && total % (chunk + 1) === 0) {
          $.pause()
          callback(rows, result => {
            rows = []

            if (limit && total === limit) {
              $.close()
              return
            }

            $.resume()
          })
        }
        else {
          rows.push(line)
        }
      }))
      .on('error', reject)
      .on('end', async () => {
        console.log('end', rows.length)
        callback(rows, respond)
      })
  })
}
