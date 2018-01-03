from app import settings
from app import app

app = app.create_app(config=settings.DevConfig)
app.run()

