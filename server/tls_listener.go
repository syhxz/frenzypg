package server

import (
	"crypto/tls"
	"fmt"
	"net"
	"time"

	"go.uber.org/zap"
)

// LoggingListener wraps a regular listener with logging
type LoggingListener struct {
	net.Listener
	logger *zap.Logger
}

// Accept accepts a connection and logs it
func (l *LoggingListener) Accept() (net.Conn, error) {
	l.logger.Debug("Waiting for new TCP connection...")
	
	conn, err := l.Listener.Accept()
	if err != nil {
		l.logger.Error("Failed to accept TCP connection", zap.Error(err))
		return nil, err
	}

	remoteAddr := conn.RemoteAddr().String()
	l.logger.Info("New TCP connection accepted", zap.String("remote_addr", remoteAddr))

	return &LoggingConn{Conn: conn, logger: l.logger, remoteAddr: remoteAddr}, nil
}

// LoggingTLSListener wraps a TLS listener with detailed logging
type LoggingTLSListener struct {
	net.Listener
	logger    *zap.Logger
	tlsConfig *tls.Config
}

// NewLoggingTLSListener creates a new TLS listener with logging
func NewLoggingTLSListener(inner net.Listener, config *tls.Config, logger *zap.Logger) *LoggingTLSListener {
	return &LoggingTLSListener{
		Listener:  inner,
		logger:    logger,
		tlsConfig: config,
	}
}

// Accept accepts a connection and handles PostgreSQL SSL negotiation
func (l *LoggingTLSListener) Accept() (net.Conn, error) {
	l.logger.Debug("Waiting for new TLS connection...")
	
	// Accept the raw connection first
	rawConn, err := l.Listener.Accept()
	if err != nil {
		l.logger.Error("Failed to accept connection", zap.Error(err))
		return nil, err
	}

	// Get remote address for logging
	remoteAddr := rawConn.RemoteAddr().String()
	l.logger.Info("New connection accepted", zap.String("remote_addr", remoteAddr))

	// Read the first message to check if it's an SSL request
	l.logger.Debug("Reading initial message", zap.String("remote_addr", remoteAddr))
	
	// Set a timeout for reading the initial message
	rawConn.SetReadDeadline(time.Now().Add(10 * time.Second))
	
	// Read the first 8 bytes (PostgreSQL message header)
	header := make([]byte, 8)
	n, err := rawConn.Read(header)
	if err != nil {
		l.logger.Error("Failed to read initial message", 
			zap.String("remote_addr", remoteAddr),
			zap.Error(err))
		rawConn.Close()
		return nil, err
	}
	
	l.logger.Debug("Initial message received", 
		zap.String("remote_addr", remoteAddr),
		zap.Int("bytes", n),
		zap.String("header_hex", fmt.Sprintf("%x", header)))

	// Check if this is a PostgreSQL SSL request
	// SSL request: length=8, code=80877103 (0x04d2162f)
	if n >= 8 {
		length := int32(header[0])<<24 | int32(header[1])<<16 | int32(header[2])<<8 | int32(header[3])
		code := int32(header[4])<<24 | int32(header[5])<<16 | int32(header[6])<<8 | int32(header[7])
		
		l.logger.Debug("PostgreSQL message parsed",
			zap.String("remote_addr", remoteAddr),
			zap.Int32("length", length),
			zap.Int32("code", code))
		
		if length == 8 && code == 80877103 { // SSL request
			l.logger.Info("SSL request received", zap.String("remote_addr", remoteAddr))
			
			// Send SSL response: 'S' for SSL supported
			_, err = rawConn.Write([]byte{'S'})
			if err != nil {
				l.logger.Error("Failed to send SSL response", 
					zap.String("remote_addr", remoteAddr),
					zap.Error(err))
				rawConn.Close()
				return nil, err
			}
			
			l.logger.Debug("SSL response sent", zap.String("remote_addr", remoteAddr))
			
			// Now start TLS handshake
			return l.performTLSHandshake(rawConn, remoteAddr)
		}
	}
	
	// If not an SSL request, create a connection that will handle the data
	l.logger.Debug("Not an SSL request, creating regular connection", zap.String("remote_addr", remoteAddr))
	
	// Create a connection that prepends the already-read data
	conn := &ConnectionWithBuffer{
		Conn:   rawConn,
		buffer: header[:n],
		logger: l.logger,
		remoteAddr: remoteAddr,
	}
	
	return conn, nil
}

// performTLSHandshake performs the actual TLS handshake after SSL negotiation
func (l *LoggingTLSListener) performTLSHandshake(rawConn net.Conn, remoteAddr string) (net.Conn, error) {
	// Wrap with TLS
	tlsConn := tls.Server(rawConn, l.tlsConfig)
	
	l.logger.Info("Starting TLS handshake", 
		zap.String("remote_addr", remoteAddr),
		zap.String("tls_min_version", l.getTLSVersionString(l.tlsConfig.MinVersion)),
		zap.Strings("cipher_suites", l.getCipherSuiteNames(l.tlsConfig.CipherSuites)))
	
	// Set handshake timeout
	deadline := time.Now().Add(30 * time.Second)
	tlsConn.SetDeadline(deadline)
	l.logger.Debug("TLS handshake timeout set", 
		zap.String("remote_addr", remoteAddr),
		zap.Time("deadline", deadline))
	
	// Perform handshake
	err := tlsConn.Handshake()
	if err != nil {
		l.logger.Error("TLS handshake failed", 
			zap.String("remote_addr", remoteAddr),
			zap.Error(err),
			zap.String("error_type", fmt.Sprintf("%T", err)))
		
		// Log additional context for common errors
		if err.Error() == "EOF" {
			l.logger.Warn("Client disconnected during TLS handshake", 
				zap.String("remote_addr", remoteAddr),
				zap.String("suggestion", "Client may not support TLS or rejected certificate"))
		}
		
		rawConn.Close()
		return nil, err
	}

	// Log successful handshake details
	state := tlsConn.ConnectionState()
	l.logger.Info("TLS handshake successful",
		zap.String("remote_addr", remoteAddr),
		zap.String("tls_version", l.getTLSVersionString(state.Version)),
		zap.String("cipher_suite", tls.CipherSuiteName(state.CipherSuite)),
		zap.Bool("resumed", state.DidResume),
		zap.Int("peer_certificates", len(state.PeerCertificates)),
		zap.String("server_name", state.ServerName))

	// Clear deadline after successful handshake
	tlsConn.SetDeadline(time.Time{})

	return &LoggingConn{Conn: tlsConn, logger: l.logger, remoteAddr: remoteAddr}, nil
}

// getTLSVersionString converts TLS version to string
func (l *LoggingTLSListener) getTLSVersionString(version uint16) string {
	switch version {
	case tls.VersionTLS10:
		return "TLS 1.0"
	case tls.VersionTLS11:
		return "TLS 1.1"
	case tls.VersionTLS12:
		return "TLS 1.2"
	case tls.VersionTLS13:
		return "TLS 1.3"
	default:
		return "Unknown"
	}
}

// LoggingConn wraps a connection with logging
type LoggingConn struct {
	net.Conn
	logger     *zap.Logger
	remoteAddr string
}

// Read logs data reading
func (c *LoggingConn) Read(b []byte) (n int, err error) {
	n, err = c.Conn.Read(b)
	if err != nil {
		c.logger.Debug("Connection read error",
			zap.String("remote_addr", c.remoteAddr),
			zap.Error(err))
	} else {
		c.logger.Debug("Data received",
			zap.String("remote_addr", c.remoteAddr),
			zap.Int("bytes", n))
	}
	return n, err
}

// Write logs data writing
func (c *LoggingConn) Write(b []byte) (n int, err error) {
	n, err = c.Conn.Write(b)
	if err != nil {
		c.logger.Debug("Connection write error",
			zap.String("remote_addr", c.remoteAddr),
			zap.Error(err))
	} else {
		c.logger.Debug("Data sent",
			zap.String("remote_addr", c.remoteAddr),
			zap.Int("bytes", n))
	}
	return n, err
}

// getCipherSuiteNames returns cipher suite names for logging
func (l *LoggingTLSListener) getCipherSuiteNames(suites []uint16) []string {
	if len(suites) == 0 {
		return []string{"default"}
	}
	
	names := make([]string, len(suites))
	for i, suite := range suites {
		names[i] = tls.CipherSuiteName(suite)
	}
	return names
}

// Close logs connection closure
func (c *LoggingConn) Close() error {
	c.logger.Info("Connection closed", zap.String("remote_addr", c.remoteAddr))
	return c.Conn.Close()
}

// ConnectionWithBuffer wraps a connection with a buffer for already-read data
type ConnectionWithBuffer struct {
	net.Conn
	buffer     []byte
	bufferUsed bool
	logger     *zap.Logger
	remoteAddr string
}

// Read reads from buffer first, then from connection
func (c *ConnectionWithBuffer) Read(b []byte) (n int, err error) {
	// If we have buffered data and haven't used it yet
	if !c.bufferUsed && len(c.buffer) > 0 {
		c.bufferUsed = true
		n = copy(b, c.buffer)
		c.logger.Debug("Reading from buffer",
			zap.String("remote_addr", c.remoteAddr),
			zap.Int("bytes", n))
		return n, nil
	}
	
	// Read from actual connection
	n, err = c.Conn.Read(b)
	if err != nil {
		c.logger.Debug("Connection read error",
			zap.String("remote_addr", c.remoteAddr),
			zap.Error(err))
	} else {
		c.logger.Debug("Data received",
			zap.String("remote_addr", c.remoteAddr),
			zap.Int("bytes", n))
	}
	return n, err
}

// Write logs data writing
func (c *ConnectionWithBuffer) Write(b []byte) (n int, err error) {
	n, err = c.Conn.Write(b)
	if err != nil {
		c.logger.Debug("Connection write error",
			zap.String("remote_addr", c.remoteAddr),
			zap.Error(err))
	} else {
		c.logger.Debug("Data sent",
			zap.String("remote_addr", c.remoteAddr),
			zap.Int("bytes", n))
	}
	return n, err
}

// Close logs connection closure
func (c *ConnectionWithBuffer) Close() error {
	c.logger.Info("Connection closed", zap.String("remote_addr", c.remoteAddr))
	return c.Conn.Close()
}
