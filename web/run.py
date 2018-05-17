from app import settings
from app import app

app = app.create_app(config=settings.DevConfig)
app.run(debug=True, host='0.0.0.0', port=8000)
