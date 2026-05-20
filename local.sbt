// sbt-git worktree workaround — JGit chokes on `gitUncommittedChanges` against linked worktrees
// because it sees a bare-repo-style gitdir layout. Override to false; we don't use the value
// inside the build (sbt-dynver computes versions from tags, not from uncommitted state).
ThisBuild / git.gitUncommittedChanges := false
