var fs = require('fs')
  , configJSON = fs.readFileSync('config.json', {encoding: 'utf-8'})
  , config = JSON.parse(configJSON);

module.exports = function(grunt) {
  grunt.initConfig({
    lambda_invoke: {
        default: {
            options: {
              event: 'testData.json'
            }
        }
    },
    lambda_package: {
        default: {
            options: {
                include_files: ['config.json']
            }
        }
    },
    lambda_deploy: {
        default: config.deploy_options
    }
  });

  grunt.loadNpmTasks('grunt-aws-lambda');

  grunt.registerTask('test', ['lambda_invoke'])

  grunt.registerTask('deploy', ['lambda_package', 'lambda_deploy']);
  grunt.registerTask('default', ['lambda_package']);
};
