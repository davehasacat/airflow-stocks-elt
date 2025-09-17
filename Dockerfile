FROM astrocrpublic.azurecr.io/runtime:3.0-10

# Add additional PYTHON dependencies here
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade -r requirements.txt

# Copy your project files first.
COPY . .

# --- NEW, CORRECTED SECTION ---
# Temporarily switch to the root user to create directories
USER root

# Create the dbt model directories
RUN mkdir -p dbt/models/source dbt/models/staging dbt/models/marts dbt/models/backtest

# Change the ownership of the new directories back to the standard 'astro' user
RUN chown -R astro:astro dbt

# Switch back to the standard 'astro' user for security
USER astro
