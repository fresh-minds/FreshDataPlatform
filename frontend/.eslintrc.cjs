module.exports = {
  root: true,
  env: {
    browser: true,
    es2021: true,
  },
  extends: ["eslint:recommended", "plugin:react/recommended", "plugin:react-hooks/recommended", "plugin:import/recommended", "prettier"],
  parserOptions: {
    ecmaVersion: "latest",
    sourceType: "module",
  },
  plugins: ["react", "react-hooks", "import"],
  settings: {
    react: {
      version: "detect",
    },
    "import/core-modules": ["keycloak-js"],
    "import/resolver": {
      node: {
        extensions: [".js", ".jsx", ".mjs", ".cjs", ".json"],
      },
    },
  },
  rules: {
    "react/prop-types": "off"
  },
};
