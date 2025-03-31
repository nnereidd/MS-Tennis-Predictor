FROM public.ecr.aws/lambda/python:3.11

# Install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy scraper code
COPY . .

# Add Chromium + ChromeDriver (from layer)
COPY chromium_layer /opt

# Set Lambda handler
CMD ["python3", "player_statistics_lambda.py"]


