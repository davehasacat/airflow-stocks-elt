FROM astrocrpublic.azurecr.io/runtime:3.0-10

# Add additional PYTHON dependencies here
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade -r requirements.txt

# Copy the rest of your project files (DAGs, dbt project, etc.)
# This ensures that any changes to your code are reflected in the image
COPY . .
