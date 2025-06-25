FROM astrocrpublic.azurecr.io/runtime:3.0-4

USER root

# Install OS packages if needed (optional)
RUN apt-get update && \
    apt-get install -y curl unzip && \
    rm -rf /var/lib/apt/lists/*

USER astro

# Create venv and install dbt + setuptools (for distutils)
RUN python -m venv /home/astro/dbt_venv && \
    /home/astro/dbt_venv/bin/pip install --no-cache-dir setuptools && \
    /home/astro/dbt_venv/bin/pip install --no-cache-dir dbt-bigquery==1.5.3