/*
 * Signal Engine Configuration
 *
 * Set your GitHub username and repo name below.
 * The dashboard fetches data directly from GitHub's raw CDN,
 * so Netlify only rebuilds when frontend code changes — not on every scan.
 */

const CONFIG = {
  // ── CHANGE THESE TO YOUR REPO ──────────────────────────────
  githubUser: 'Br4cky',
  githubRepo: 'polymarket-signal-engine',
  githubBranch: 'main',
  // ───────────────────────────────────────────────────────────

  get dataBase() {
    return `https://raw.githubusercontent.com/${this.githubUser}/${this.githubRepo}/${this.githubBranch}/data/`;
  }
};
