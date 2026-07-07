#!/bin/bash
set -e

# FrenzyPG installation script
# Run as root: sudo bash deploy/install.sh

BINARY_SRC="bin/frenzy"
BINARY_DST="/usr/local/bin/frenzy"
CONFIG_DIR="/etc/frenzy"
LOG_DIR="/var/log/frenzy"
RUN_DIR="/var/run/frenzy"
SERVICE_USER="frenzy"

echo "=== Installing FrenzyPG ==="

# Check binary exists
if [ ! -f "$BINARY_SRC" ]; then
    echo "Error: binary not found at $BINARY_SRC. Run 'make build' first."
    exit 1
fi

# Create service user
if ! id "$SERVICE_USER" &>/dev/null; then
    echo "Creating user: $SERVICE_USER"
    useradd --system --no-create-home --shell /bin/false "$SERVICE_USER"
fi

# Install binary
echo "Installing binary to $BINARY_DST"
cp "$BINARY_SRC" "$BINARY_DST"
chmod 755 "$BINARY_DST"

# Create directories
echo "Creating directories"
mkdir -p "$CONFIG_DIR" "$LOG_DIR" "$RUN_DIR"
chown "$SERVICE_USER:$SERVICE_USER" "$LOG_DIR" "$RUN_DIR"

# Install config (do not overwrite existing)
if [ ! -f "$CONFIG_DIR/frenzy.yaml" ]; then
    echo "Installing default config to $CONFIG_DIR/frenzy.yaml"
    cp deploy/frenzy.yaml.example "$CONFIG_DIR/frenzy.yaml"
    chown root:"$SERVICE_USER" "$CONFIG_DIR/frenzy.yaml"
    chmod 640 "$CONFIG_DIR/frenzy.yaml"
else
    echo "Config already exists at $CONFIG_DIR/frenzy.yaml, skipping"
fi

# Install credentials template (do not overwrite existing)
if [ ! -f "$CONFIG_DIR/credentials" ]; then
    echo "Installing credentials template to $CONFIG_DIR/credentials"
    cp deploy/credentials.example "$CONFIG_DIR/credentials"
    chown root:"$SERVICE_USER" "$CONFIG_DIR/credentials"
    chmod 640 "$CONFIG_DIR/credentials"
    echo "  >>> IMPORTANT: Edit $CONFIG_DIR/credentials with actual passwords"
else
    echo "Credentials file already exists, skipping"
fi

# Install systemd service
echo "Installing systemd service"
cp deploy/frenzy.service /etc/systemd/system/frenzy.service
systemctl daemon-reload

echo ""
echo "=== Installation complete ==="
echo ""
echo "Next steps:"
echo "  1. Edit $CONFIG_DIR/frenzy.yaml with your database settings"
echo "  2. Edit $CONFIG_DIR/credentials with actual passwords"
echo "  3. Start the service: systemctl start frenzy"
echo "  4. Enable on boot:    systemctl enable frenzy"
echo "  5. Check status:      systemctl status frenzy"
echo "  6. View logs:         journalctl -u frenzy -f"
