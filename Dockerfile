FROM python:3.11

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade --no-cache-dir -r requirements.txt

RUN apt-get update && apt-get install -y \
    libreoffice \
    libreoffice-calc \
    imagemagick \
    ghostscript \
    && rm -rf /var/lib/apt/lists/*

RUN sed -i 's/policy domain="coder" rights="none" pattern="PDF"/policy domain="coder" rights="read|write" pattern="PDF"/' /etc/ImageMagick-6/policy.xml

# Copy custom font file (assuming it's named custom_font.ttf)
COPY asset/Calibri.ttf /usr/share/fonts/truetype/

# Refresh font cache
RUN fc-cache -f -v

COPY . .

CMD ["gunicorn", "--bind", "0.0.0.0:5002", "--timeout", "1200", "app:app"]
