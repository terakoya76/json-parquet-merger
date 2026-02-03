'use strict';

module.exports = [
  ...require('gts'),
  {
    ignores: [
      "build",
      "dist/",
      "*.config.ts",
      "*.config.js",
      "coverage/",
      "node_modules/",
    ],
  },
  {
    "files": ["*.test.ts"],
    "rules": {
      "n/no-unpublished-import": "off"
    }
  },
];
