# api/Dockerfile
FROM python:3.12-slim

# 1. Instala o próprio uv apenas uma vez
RUN pip install --no-cache-dir uv

WORKDIR /app

# 2. Copia manifestos antes do código p/ cache eficiente
COPY pyproject.toml uv.lock ./

# 3. Resolve e instala dependências dentro do sistema do contêiner
ENV UV_PROJECT_ENVIRONMENT=system
RUN uv sync --locked

# 4. Copia a aplicação (último passo para não invalidar cache à toa)
COPY app/ .

CMD ["uv", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
