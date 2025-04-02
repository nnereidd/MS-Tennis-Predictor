FROM public.ecr.aws/lambda/python:3.11

# Install system dependencies
RUN yum install -y \
    atk cups-libs gtk3 libXcomposite alsa-lib libXcursor libXdamage \
    libXext libXrandr libXScrnSaver libXtst pango dbus-glib nss mesa-libgbm \
    xorg-x11-server-Xvfb libX11 libXrender unzip jq wget

# Copy and run installer script
COPY chrome-installer.sh /chrome-installer.sh
RUN chmod +x /chrome-installer.sh && /chrome-installer.sh && rm /chrome-installer.sh

# Install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Set handler
CMD ["match_charting_project_lambda.handler"]



