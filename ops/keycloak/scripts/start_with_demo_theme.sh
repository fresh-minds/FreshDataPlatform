#!/bin/sh
set -eu

THEME_DIR="/opt/keycloak/themes/odp-demo-autologin/login"
AUTO_LOGIN_RAW="${KEYCLOAK_DEMO_AUTO_LOGIN:-false}"
AUTO_LOGIN="$(printf '%s' "$AUTO_LOGIN_RAW" | tr '[:upper:]' '[:lower:]')"
DEMO_USERNAME="${KEYCLOAK_DEMO_AUTOLOGIN_USERNAME:-odp-admin}"
DEMO_PASSWORD="${KEYCLOAK_DEFAULT_USER_PASSWORD:-admin}"

if [ "$AUTO_LOGIN" = "1" ] || [ "$AUTO_LOGIN" = "yes" ] || [ "$AUTO_LOGIN" = "on" ]; then
  AUTO_LOGIN="true"
fi
if [ "$AUTO_LOGIN" != "true" ]; then
  AUTO_LOGIN="false"
fi

html_escape() {
  printf '%s' "$1" | sed \
    -e 's/&/\&amp;/g' \
    -e 's/"/\&quot;/g' \
    -e "s/'/\&#39;/g" \
    -e 's/</\&lt;/g' \
    -e 's/>/\&gt;/g'
}

DEMO_USERNAME_ESCAPED="$(html_escape "$DEMO_USERNAME")"
DEMO_PASSWORD_ESCAPED="$(html_escape "$DEMO_PASSWORD")"

mkdir -p "$THEME_DIR"

cat > "$THEME_DIR/theme.properties" <<'EOF'
parent=keycloak.v2
EOF

cat > "$THEME_DIR/login.ftl" <<EOF
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Keycloak Demo SSO</title>
    <style>
      body { font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin: 0; min-height: 100vh; display: grid; place-items: center; background: #f6f8fb; color: #111827; }
      .box { width: min(92vw, 420px); background: #fff; border: 1px solid #d6dde7; border-radius: 12px; padding: 20px; }
      h1 { margin: 0 0 8px 0; font-size: 1rem; }
      p { margin: 0; color: #596172; font-size: 0.92rem; }
      .hidden-form { display: none; }
      .manual { margin-top: 14px; }
      label { display: block; margin-bottom: 8px; font-size: 0.85rem; color: #374151; }
      input { width: 100%; box-sizing: border-box; padding: 8px 10px; border-radius: 8px; border: 1px solid #cbd5e1; }
      button { margin-top: 10px; width: 100%; padding: 10px 12px; border-radius: 8px; border: 0; background: #0f4c81; color: #fff; font-weight: 600; cursor: pointer; }
    </style>
  </head>
  <body>
    <div class="box">
      <h1>Signing in to demo session…</h1>
      <p id="status-text">Preparing secure sign-in.</p>

      <form id="kc-demo-autologin-form" class="hidden-form" action="\${url.loginAction}" method="post">
        <input type="text" name="username" value="$DEMO_USERNAME_ESCAPED" autocomplete="username" />
        <input type="password" name="password" value="$DEMO_PASSWORD_ESCAPED" autocomplete="current-password" />
      </form>

      <form id="kc-manual-login-form" class="manual" action="\${url.loginAction}" method="post">
        <label>
          Username
          <input type="text" name="username" value="$DEMO_USERNAME_ESCAPED" autocomplete="username" />
        </label>
        <label>
          Password
          <input type="password" name="password" value="$DEMO_PASSWORD_ESCAPED" autocomplete="current-password" />
        </label>
        <button type="submit">Sign in</button>
      </form>
    </div>

    <script>
      const autoLoginEnabled = $AUTO_LOGIN;
      const autoForm = document.getElementById('kc-demo-autologin-form');
      const manualForm = document.getElementById('kc-manual-login-form');
      const statusText = document.getElementById('status-text');

      if (autoLoginEnabled) {
        manualForm.style.display = 'none';
        statusText.textContent = 'Signing in automatically as demo admin…';
        window.setTimeout(() => autoForm.submit(), 30);
      } else {
        statusText.textContent = 'Demo auto-login disabled. Use manual sign-in below.';
      }
    </script>
  </body>
</html>
EOF

exec /opt/keycloak/bin/kc.sh start-dev --http-port=8090 --import-realm
