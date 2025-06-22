# Start from the official Ollama image
FROM ollama/ollama:latest

# Set the working directory (optional, but good practice for consistency)
WORKDIR /root/.ollama

# Pastikan PATH diatur dengan benar untuk fase BUILD
RUN /bin/bash -c "export PATH=$PATH:/usr/local/bin:/usr/bin; \
    /usr/bin/ollama serve > /dev/null 2>&1 & \
    sleep 15 && \
    /usr/bin/ollama pull deepseek-r1:1.5b && \
    pkill ollama"

# **INI ADALAH PERBAIKAN KRITIS UNTUK RUNTIME ENTRYPOINT:**
# Gunakan jalur absolut yang ditemukan: /usr/bin/ollama
ENTRYPOINT ["/usr/bin/ollama", "serve"]