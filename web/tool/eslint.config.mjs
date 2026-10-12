import nextConfig from "../service/submitqueue/node_modules/eslint-config-next/dist/index.js";

const parser = nextConfig.find((config) => config.name === "next/typescript")
  .languageOptions.parser;
const react = nextConfig.find((config) => config.plugins?.react).plugins.react;

export default [
  { ignores: ["dist/**", "node_modules/**"] },
  {
    files: ["**/*.ts", "**/*.tsx"],
    languageOptions: {
      parser,
      parserOptions: {
        ecmaVersion: "latest",
        sourceType: "module",
        ecmaFeatures: { jsx: true },
      },
    },
    plugins: { react },
    rules: {
      "react/jsx-key": "error",
      "react/jsx-no-duplicate-props": "error",
      "no-unreachable": "error",
      "no-dupe-args": "error",
    },
  },
];
