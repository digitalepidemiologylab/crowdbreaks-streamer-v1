from flask import Flask

app = Flask(__name__, instance_relative_config=True)

# configs
app.config.from_object('config')
# secret configs
app.config.from_pyfile('config.py')

# connect db to app

@app.route('/', methods=['GET'])
def index():
    return "hello world!!!"


if __name__ == '__main__':
    app.run()
