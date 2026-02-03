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
    "files": ["**/*.ts", "**/*.tsx"],
    "rules": {
      // Prevent "as unknown as X" double type assertions which bypass type checking
      "no-restricted-syntax": [
        "error",
        {
          "selector": "TSAsExpression > TSAsExpression[typeAnnotation.type='TSUnknownKeyword']",
          "message": "Avoid 'as unknown as X' type assertions. Use proper typing, create a typed factory function, or use a single 'as X' assertion with proper type compatibility."
        }
      ]
    }
  },
  {
    "files": ["**/*.test.ts"],
    "rules": {
      "n/no-unpublished-import": "off"
    }
  },
];
