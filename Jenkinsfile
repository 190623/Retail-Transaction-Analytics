pipeline {
    agent any

    stages {

        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Build Docker Images') {
            steps {
                sh 'docker compose build'
            }
        }

        stage('Deploy Application') {
            steps {
                sh 'docker compose up -d'
            }
        }

        stage('Verify Containers') {
            steps {
                sh 'docker compose ps'
            }
        }

      stage('Verify Backend') {
    steps {
        sh '''
            for i in $(seq 1 12); do
                if curl -f http://localhost:5000/; then
                    echo "Backend is ready."
                    exit 0
                fi

                echo "Backend is not ready yet. Waiting 5 seconds..."
                sleep 5
            done

            echo "Backend did not become ready."
            exit 1
        '''
    }
}

        stage('Verify Frontend') {
            steps {
                sh 'curl -f http://localhost:3000'
            }
        }
    }

    post {
        always {
            sh 'docker compose ps'
        }

        success {
            echo 'Retail Transaction Analytics deployment completed successfully.'
        }

        failure {
            echo 'Retail Transaction Analytics deployment failed.'
        }
    }
}