import { handler } from '../src/index'

handler({ foo: 'bar' }).then(resp => console.log('Lambda fn completed with response:', resp))