from app import settings
from app import app

app = app.create_app(config=settings.DevConfig)
# app = app.create_app()
app.run(host='0.0.0.0', port=8000)
# app.run(host='localhost', port=8000)

