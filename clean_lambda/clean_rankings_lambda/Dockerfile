FROM public.ecr.aws/lambda/python:3.11

WORKDIR /var/task

# Install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Set handler
CMD ["rankings_lambda.handler"]