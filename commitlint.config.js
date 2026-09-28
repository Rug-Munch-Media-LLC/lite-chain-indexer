// RugMunch fleet commitlint — lite-chain-indexer
//
// RESTORED 2026-09-28. This file was MISSING while .lefthook.yml still ran
// `commitlint --edit "$1"` on commit-msg, so commitlint aborted with
// "Please add rules to your commitlint.config.js" and EVERY commit in this
// repo was rejected. The hook was green-on-nothing: no commit had been made
// through it since the hook was added.
//
// Deliberately self-contained (no `extends`), because this repo has no
// node_modules and therefore cannot resolve @commitlint/config-conventional.
// It enforces the parts of the convention that matter for a commit header
// without requiring an install. See rmi-backend/commitlint.config.js, which
// uses the same self-contained shape.
module.exports = {
	rules: {
		// header must be `type(scope): subject` — conventional commits
		"type-enum": [
			2,
			"always",
			[
				"feat",
				"fix",
				"docs",
				"style",
				"refactor",
				"perf",
				"test",
				"build",
				"ci",
				"chore",
				"revert",
				"ops",
				"security",
			],
		],
		"type-empty": [2, "never"],
		"subject-empty": [2, "never"],
		"header-max-length": [2, "always", 120],
	},
};
