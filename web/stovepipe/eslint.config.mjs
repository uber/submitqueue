import config from "../tool/eslint.config.mjs";

export default [...config, { rules: { "max-statements-per-line": ["error", { max: 1 }] } }];
