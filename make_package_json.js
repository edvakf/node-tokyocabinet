// http://wiki.commonjs.org/wiki/Packages/1.0
// $ node make_package_json.js

package_info = {
  name : 'node-tokyocabinet',
  description : 'Tokyo Cabinet binding for node.js',
  version : '0.0.1',
  keywords : [
    'node.js', 'tokyo cabinet'
  ],
  maintainers : [{
    name : 'Atsushi Takayama',
    email : 'taka.atsushi@gmail.com',
    web : 'http://d.hatena.ne.jp/edvakf/'
  }],
  licenses : [{
    type : 'MIT',
    url : 'http://www.opensource.org/licenses/mit-license.php'
  }],
  repositories : [{
    type : 'git',
    url : 'http://github.com/edvakf/node-tokyocabinet'
  }],
  dependencies : {
    lib : 'tokyocabinet',
  },
  engines : ['node']
}

require('fs').writeFile(
  'package.json', 
  JSON.stringify(package_info, null, 4)
);
