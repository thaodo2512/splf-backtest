# Multi-arch CPU image (amd64/arm64) using micromamba
FROM mambaorg/micromamba:1.5.7

# Make the base env active in subsequent layers
ARG MAMBA_DOCKERFILE_ACTIVATE=1

WORKDIR /app

# Create env with all deps
COPY --chown=$MAMBA_USER:$MAMBA_USER environment.yml /tmp/environment.yml
RUN micromamba install -y -n base -f /tmp/environment.yml && \
    micromamba clean --all --yes

# Copy project
COPY --chown=$MAMBA_USER:$MAMBA_USER . /app

# Useful ports (Jupyter)
EXPOSE 8888

# Ensure project is importable
ENV PYTHONPATH=/app

# Default to a shell; override with `docker run ... <command>`
CMD ["bash"]

