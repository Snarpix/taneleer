# Taneleer

Taneleer is artifact manager for making cacheable and reproducible builds

It's currently in heavy development, no stability guaranties.

## Goals

* Atomically commit artifacts, prohibit any modification
* Allow access using local filesystem and also http and other protocols
* Docker containers as artifacts
* Attach metadata about build time, and list all the sources
* "Give me latest build of this artifact from this commit"

## Usage

1. Create build job(for example multibranch pipeline jenkins):
```
node { 
    def results
    stage('Checkout') { 
        results = checkout scm
    }

    def artifact_uuid
    def artifact_url
    stage('Reserve') { 
        sh "printenv | sort"
        def res = sh(returnStdout: true, script: "tcli artifact reserve rust_builder --src main,git,${results.GIT_URL},${results.GIT_COMMIT} --tag builder:jenkins --tag branch:${env.BRANCH_NAME} --tag build_id:${env.BUILD_ID}").trim().split('\n')
        artifact_uuid = res[0]
        artifact_url = res[1]
    }

    stage('Build') { 
        sh "docker build -t ${artifact_url} rust_builder"
    }

    stage('Push') { 
        sh "docker push ${artifact_url}"
    }

    stage('Commit') { 
        sh "tcli artifact commit ${artifact_uuid}"
    }
}

```
2. In other job get last artifact or request a build
```
node { 
    def results
    stage('Checkout') { 
        results = checkout scm
    }

    def artifact_uuid
    def artifact_url
    def commit = 'fbedbd91c89194519b857ea12d691753bab515f9'
    def repo = '/Users/snarpix/Documents/rust_builder'
    stage('Get') { 
        sh "printenv | sort"
        try {
            def res = sh(returnStdout: true, script: "tcli artifact last rust_builder --src main,git,$repo,$commit").trim().split('\n')
            reserve_uuid = res[0]
            artifact_uuid = res[1]
            artifact_url = res[2]
        } catch (e) {
            // Use bitbucket or gihub api to add a tag
            // If no API - use clone filter
            // Or just clone whole repo to add a tag
            sh "git clone $repo"
            dir('rust_builder') {
                sh "git co $commit"
                sh "git tag -a tag_123 -m 'tag_123'"
                sh "git push origin tag_123"
            }
            build(job: "rust_builder", wait: false)
            // No way to wait for multibranch pipeline scan to finish
            // Poll
            for (int i=0; i < 13; i++) {
                try {
                    build(job: "rust_builder/tag_123")
                    break;
                } catch(hudson.AbortException nested_e) {
                    if (nested_e.getMessage() != "No item named rust_builder/tag_123 found") {
                        throw nested_e;
                    }
                    sleep(10);
                }
            }
            // Now build is finished and we can use new artifact
            def res = sh(returnStdout: true, script: "tcli artifact last rust_builder --src main,git,$repo,$commit").trim().split('\n')
            reserve_uuid = res[0]
            artifact_uuid = res[1]
            artifact_url = res[2]
        }
        sh "echo $reserve_uuid $artifact_uuid $artifact_url"
    }
}
```

## Contributing

By submitting a contribution to this project you accept that your contribution will be licensed under AGPL-3.0, AND you grant maintainer a right to relicense  your contribution under any other licence. 

This is need for future-proofing of the project. If I need to use GPL 2.0 lib, I can just relicense whole project under GPL 2.0, without asking each contributor individualy.

### In formal words:
Maintainer is Stanislav Shmarov <github@snarpix.com>.
Contribution is the code, documentation or other original works submitted to be included in this project.
Contributor is person or company submitting the Contribution.
By submitting Contribution to this project, the Contributor grants to Maintainer a sublicensable, irrevocable, perpetual, worldwide, non-exclusive, royalty-free and fully paid-up copyright and trade secret license to reproduce, adapt, translate, modify, and prepare 
derivative works of, publicly display, publicly perform, sublicense, make available and distribute his Contribution and any derivative works
thereof under license terms of Maintainer's choosing including any Open Source Software or Commercial license.

## License
[AGPL-3.0](https://www.gnu.org/licenses/agpl-3.0.txt)