FROM alpine
# MAINTAINER David Personette <dperson@gmail.com>
LABEL maintainer="David Personette <dperson@gmail.com>"

# Install tor and privoxy
RUN apk --no-cache --no-progress upgrade && \
    apk --no-cache --no-progress add bash curl privoxy shadow tini tor tzdata torsocks && \
    apk --no-cache --no-progress add busybox-suid && \
    apk --no-cache --no-progress add cronie && \
    mv /etc/privoxy/config.new /etc/privoxy/config && \
    mv /etc/privoxy/default.action.new /etc/privoxy/default.action && \
    mv /etc/privoxy/user.action.new /etc/privoxy/user.action && \
    mv /etc/privoxy/default.filter.new /etc/privoxy/default.filter && \
    mv /etc/privoxy/user.filter.new /etc/privoxy/user.filter && \
    mv /etc/privoxy/regression-tests.action.new /etc/privoxy/regression-tests.action && \
    mv /etc/privoxy/trust.new /etc/privoxy/trust && \
    mv /etc/privoxy/match-all.action.new /etc/privoxy/match-all.action && \
    file='/etc/privoxy/config' && \
    sed -i 's|^\(accept-intercepted-requests\) .*|\1 1|' $file && \
    sed -i '/^listen/s|127\.0\.0\.1||' $file && \
    sed -i '/^listen.*::1/s|^|#|' $file && \
    sed -i 's|^\(logfile\)|#\1|' $file && \
    sed -i 's|^#\(log-messages\)|\1|' $file && \
    sed -i 's|^#\(log-highlight-messages\)|\1|' $file && \
    sed -i '/forward *localhost\//a forward-socks5t / 127.0.0.1:9050 .' $file && \
    sed -i '/^forward-socks5t \//a forward 172.16.*.*/ .' $file && \
    sed -i '/^forward 172\.16\.\*\.\*\//a forward 172.17.*.*/ .' $file && \
    sed -i '/^forward 172\.17\.\*\.\*\//a forward 172.18.*.*/ .' $file && \
    sed -i '/^forward 172\.18\.\*\.\*\//a forward 172.19.*.*/ .' $file && \
    sed -i '/^forward 172\.19\.\*\.\*\//a forward 172.20.*.*/ .' $file && \
    sed -i '/^forward 172\.20\.\*\.\*\//a forward 172.21.*.*/ .' $file && \
    sed -i '/^forward 172\.21\.\*\.\*\//a forward 172.22.*.*/ .' $file && \
    sed -i '/^forward 172\.22\.\*\.\*\//a forward 172.23.*.*/ .' $file && \
    sed -i '/^forward 172\.23\.\*\.\*\//a forward 172.24.*.*/ .' $file && \
    sed -i '/^forward 172\.24\.\*\.\*\//a forward 172.25.*.*/ .' $file && \
    sed -i '/^forward 172\.25\.\*\.\*\//a forward 172.26.*.*/ .' $file && \
    sed -i '/^forward 172\.26\.\*\.\*\//a forward 172.27.*.*/ .' $file && \
    sed -i '/^forward 172\.27\.\*\.\*\//a forward 172.28.*.*/ .' $file && \
    sed -i '/^forward 172\.28\.\*\.\*\//a forward 172.29.*.*/ .' $file && \
    sed -i '/^forward 172\.29\.\*\.\*\//a forward 172.30.*.*/ .' $file && \
    sed -i '/^forward 172\.30\.\*\.\*\//a forward 172.31.*.*/ .' $file && \
    sed -i '/^forward 172\.31\.\*\.\*\//a forward 10.*.*.*/ .' $file && \
    sed -i '/^forward 10\.\*\.\*\.\*\//a forward 192.168.*.*/ .' $file && \
    sed -i '/^forward 192\.168\.\*\.\*\//a forward 127.*.*.*/ .' $file && \
    sed -i '/^forward 127\.\*\.\*\.\*\//a forward localhost/ .' $file && \
    echo 'AutomapHostsOnResolve 1' >>/etc/tor/torrc && \
    echo 'ControlPort 9051' >>/etc/tor/torrc && \
    echo 'ControlSocket /etc/tor/run/control' >>/etc/tor/torrc && \
    echo 'ControlSocketsGroupWritable 1' >>/etc/tor/torrc && \
    echo 'DNSPort 5353' >>/etc/tor/torrc && \
    echo 'DataDirectory /var/lib/tor' >>/etc/tor/torrc && \
    echo 'ExitPolicy reject *:*' >>/etc/tor/torrc && \
    echo 'Log notice stderr' >>/etc/tor/torrc && \
    echo 'RunAsDaemon 0' >>/etc/tor/torrc && \
    echo 'SocksPort 0.0.0.0:9050 IsolateDestAddr' >>/etc/tor/torrc && \
    echo 'TransPort 0.0.0.0:9040' >>/etc/tor/torrc && \
    echo 'User tor' >>/etc/tor/torrc && \
    echo 'VirtualAddrNetworkIPv4 10.192.0.0/10' >>/etc/tor/torrc && \

    # —— ensure tor user/group exist (idempotent + _tor fallback) ——
    # prefer 'tor', fallback to '_tor' if it already exists
    grep -q '^tor:' /etc/group || addgroup -S tor; \
    id -u tor >/dev/null 2>&1 || adduser -S -H -D -s /sbin/nologin -G tor tor || true; \
    TOR_USER=tor; \
    getent passwd _tor >/dev/null && TOR_USER=_tor; \
    mkdir -p /etc/tor/run /var/lib/tor; \
    chown -Rh ${TOR_USER}:${TOR_USER} /var/lib/tor /etc/tor/run; \
    chmod 0750 /etc/tor/run; \

    # mkdir -p /etc/tor/run && \
    # chown -Rh tor. /var/lib/tor /etc/tor/run && \
    # chmod 0750 /etc/tor/run && \
    rm -rf /tmp/*

COPY torproxy.sh /usr/bin/
COPY send_newnym.sh /usr/local/bin/

RUN chmod +x /usr/local/bin/send_newnym.sh

RUN echo "0 * * * * /usr/local/bin/send_newnym.sh" > /etc/cron.d/newnym

RUN chmod 0644 /etc/cron.d/newnym

RUN crontab /etc/cron.d/newnym

RUN mkdir -p /var/log && touch /var/log/send_newnym.log

EXPOSE 8118 9050 9051

HEALTHCHECK --interval=60s --timeout=15s --start-period=20s \
            CMD curl -sx localhost:8118 'https://check.torproject.org/' | \
            grep -qm1 Congratulations

VOLUME ["/etc/tor", "/var/lib/tor"]

#ENTRYPOINT ["/sbin/tini", "--", "/usr/bin/torproxy.sh"]
ENTRYPOINT ["sh", "-c", "crond && tail -f /var/log/send_newnym.log & /sbin/tini -- /usr/bin/torproxy.sh"]