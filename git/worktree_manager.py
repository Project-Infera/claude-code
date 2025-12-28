"""
Git Worktree Manager for Claude Code Multi-Agent System

Manages git worktrees for parallel agent development with:
- Isolated working directories per agent
- Shared .git directory (efficient, ~1MB per worktree)
- Conflict detection before merging
- SQLite tracking of active worktrees
- Automatic cleanup of stale worktrees

Author: Project Infera
Date: 2025-12-27
Version: 1.0.0
"""

import logging
import os
import sqlite3
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


# ============================================================================
# Data Classes
# ============================================================================


@dataclass
class WorktreeInfo:
    """
    Information about a git worktree.

    Attributes:
        agent_id: Unique agent identifier
        path: Absolute path to worktree directory
        branch: Git branch name for this worktree
        created_at: Timestamp when worktree was created
        last_active: Timestamp of last activity
        status: Active, stale, or merged
    """

    agent_id: str
    path: Path
    branch: str
    created_at: datetime
    last_active: datetime
    status: str = "active"  # active, stale, merged


@dataclass
class ConflictFile:
    """
    Represents a file with merge conflicts.

    Attributes:
        path: Relative path to conflicting file
        conflict_type: Type of conflict (content, rename, delete)
        our_version: SHA of our version
        their_version: SHA of their version
        base_version: SHA of common ancestor (if available)
    """

    path: str
    conflict_type: str  # "content", "rename", "delete"
    our_version: Optional[str] = None
    their_version: Optional[str] = None
    base_version: Optional[str] = None


# ============================================================================
# Git Worktree Manager
# ============================================================================


class GitWorktreeManager:
    """
    Manages git worktrees for parallel Claude Code agent development.

    Features:
    - Create/delete git worktrees with isolated working directories
    - Share .git directory across all worktrees (disk efficient)
    - Track active worktrees in SQLite database
    - Detect merge conflicts before attempting merge
    - Cleanup stale worktrees (agents that crashed)
    - Validate git repository state

    Disk Usage:
    - Base repository: ~100MB
    - Per worktree: ~1MB (shared .git)
    - SQLite database: < 1MB

    Example:
        ```python
        manager = GitWorktreeManager("/workspace/ifers")
        worktree_path = manager.create_worktree("agent-1")
        # Agent works in worktree_path
        conflicts = manager.detect_conflicts("agent-1")
        if not conflicts:
            manager.merge_worktree("agent-1")
        manager.remove_worktree("agent-1")
        ```
    """

    def __init__(
        self,
        repo_path: Path | str,
        worktree_base_dir: Optional[Path | str] = None,
        db_path: Optional[Path | str] = None,
    ):
        """
        Initialize GitWorktreeManager.

        Args:
            repo_path: Path to main git repository
            worktree_base_dir: Directory for worktrees (default: repo_path/../worktrees)
            db_path: Path to SQLite database (default: repo_path/.worktrees.db)

        Raises:
            ValueError: If repo_path is not a git repository
        """
        self.repo_path = Path(repo_path).resolve()

        if not (self.repo_path / ".git").exists():
            raise ValueError(f"Not a git repository: {self.repo_path}")

        # Default worktree base directory
        if worktree_base_dir is None:
            self.worktree_base_dir = self.repo_path.parent / "worktrees"
        else:
            self.worktree_base_dir = Path(worktree_base_dir).resolve()

        self.worktree_base_dir.mkdir(parents=True, exist_ok=True)

        # SQLite database for tracking worktrees
        if db_path is None:
            self.db_path = self.repo_path / ".worktrees.db"
        else:
            self.db_path = Path(db_path)

        self._init_database()
        logger.info(
            f"GitWorktreeManager initialized: repo={self.repo_path}, "
            f"worktree_base={self.worktree_base_dir}"
        )

    def _init_database(self):
        """Initialize SQLite database for worktree tracking."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS worktrees (
                agent_id TEXT PRIMARY KEY,
                path TEXT NOT NULL,
                branch TEXT NOT NULL,
                created_at TEXT NOT NULL,
                last_active TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'active'
            )
        """
        )

        conn.commit()
        conn.close()
        logger.info(f"SQLite database initialized: {self.db_path}")

    def create_worktree(self, agent_id: str, base_branch: str = "main") -> Path:
        """
        Create new git worktree for agent.

        Args:
            agent_id: Unique agent identifier
            base_branch: Base branch to branch from (default: "main")

        Returns:
            Path to newly created worktree

        Raises:
            ValueError: If worktree already exists for this agent
            subprocess.CalledProcessError: If git worktree add fails

        Example:
            ```python
            manager = GitWorktreeManager("/workspace/ifers")
            worktree_path = manager.create_worktree("agent-1")
            # worktree_path = /workspace/worktrees/worktree-agent-1
            ```
        """
        # Check if worktree already exists
        existing = self._get_worktree(agent_id)
        if existing:
            raise ValueError(f"Worktree already exists for agent {agent_id}: {existing.path}")

        worktree_path = self.worktree_base_dir / f"worktree-{agent_id}"
        branch_name = f"agent-{agent_id}"

        # Create worktree with new branch
        logger.info(f"Creating worktree for agent {agent_id}: {worktree_path}")

        try:
            subprocess.run(
                [
                    "git",
                    "worktree",
                    "add",
                    str(worktree_path),
                    "-b",
                    branch_name,
                    base_branch,
                ],
                cwd=self.repo_path,
                check=True,
                capture_output=True,
                text=True,
            )

            # Track in database
            now = datetime.now(timezone.utc).isoformat()
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO worktrees (agent_id, path, branch, created_at, last_active, status)
                VALUES (?, ?, ?, ?, ?, ?)
            """,
                (agent_id, str(worktree_path), branch_name, now, now, "active"),
            )
            conn.commit()
            conn.close()

            logger.info(f"Worktree created successfully: {worktree_path}")
            return worktree_path

        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to create worktree: {e.stderr}")
            raise

    def remove_worktree(self, agent_id: str, force: bool = False):
        """
        Remove git worktree and associated branch.

        Args:
            agent_id: Agent identifier
            force: Force removal even if worktree has uncommitted changes

        Raises:
            ValueError: If worktree does not exist
            subprocess.CalledProcessError: If git worktree remove fails

        Example:
            ```python
            manager.remove_worktree("agent-1")
            ```
        """
        worktree = self._get_worktree(agent_id)
        if not worktree:
            raise ValueError(f"Worktree does not exist for agent {agent_id}")

        logger.info(f"Removing worktree for agent {agent_id}: {worktree.path}")

        try:
            # Remove worktree
            cmd = ["git", "worktree", "remove", str(worktree.path)]
            if force:
                cmd.append("--force")

            subprocess.run(
                cmd,
                cwd=self.repo_path,
                check=True,
                capture_output=True,
                text=True,
            )

            # Delete branch
            subprocess.run(
                ["git", "branch", "-D", worktree.branch],
                cwd=self.repo_path,
                check=True,
                capture_output=True,
                text=True,
            )

            # Remove from database
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("DELETE FROM worktrees WHERE agent_id = ?", (agent_id,))
            conn.commit()
            conn.close()

            logger.info(f"Worktree removed successfully: {worktree.path}")

        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to remove worktree: {e.stderr}")
            raise

    def list_worktrees(self) -> list[WorktreeInfo]:
        """
        List all active worktrees.

        Returns:
            List of WorktreeInfo objects

        Example:
            ```python
            worktrees = manager.list_worktrees()
            for wt in worktrees:
                print(f"{wt.agent_id}: {wt.path} ({wt.status})")
            ```
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT agent_id, path, branch, created_at, last_active, status
            FROM worktrees
            ORDER BY created_at DESC
        """
        )

        worktrees = []
        for row in cursor.fetchall():
            worktrees.append(
                WorktreeInfo(
                    agent_id=row[0],
                    path=Path(row[1]),
                    branch=row[2],
                    created_at=datetime.fromisoformat(row[3]),
                    last_active=datetime.fromisoformat(row[4]),
                    status=row[5],
                )
            )

        conn.close()
        return worktrees

    def detect_conflicts(
        self, agent_id: str, target_branch: str = "main"
    ) -> list[ConflictFile]:
        """
        Detect merge conflicts before attempting merge.

        Uses `git merge-tree` to perform a dry-run merge and detect conflicts
        without modifying the working tree.

        Args:
            agent_id: Agent identifier
            target_branch: Target branch to merge into (default: "main")

        Returns:
            List of ConflictFile objects (empty if no conflicts)

        Raises:
            ValueError: If worktree does not exist

        Example:
            ```python
            conflicts = manager.detect_conflicts("agent-1")
            if conflicts:
                print(f"Conflicts detected in {len(conflicts)} files:")
                for cf in conflicts:
                    print(f"  {cf.path} ({cf.conflict_type})")
            else:
                print("No conflicts, safe to merge")
            ```
        """
        worktree = self._get_worktree(agent_id)
        if not worktree:
            raise ValueError(f"Worktree does not exist for agent {agent_id}")

        logger.info(f"Detecting conflicts for agent {agent_id}: {worktree.branch} → {target_branch}")

        try:
            # Run git merge-tree (dry-run merge)
            result = subprocess.run(
                ["git", "merge-tree", target_branch, worktree.branch],
                cwd=self.repo_path,
                check=False,
                capture_output=True,
                text=True,
            )

            # Parse output for conflicts
            conflicts = []
            if result.returncode != 0 or "conflict" in result.stdout.lower():
                # Simple parsing: look for conflict markers in output
                lines = result.stdout.split("\n")
                for line in lines:
                    if line.startswith("changed in both"):
                        # Extract file path
                        parts = line.split()
                        if len(parts) >= 4:
                            file_path = parts[-1]
                            conflicts.append(
                                ConflictFile(
                                    path=file_path,
                                    conflict_type="content",
                                )
                            )

            logger.info(f"Conflict detection complete: {len(conflicts)} conflicts found")
            return conflicts

        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to detect conflicts: {e.stderr}")
            raise

    def _get_worktree(self, agent_id: str) -> Optional[WorktreeInfo]:
        """
        Get worktree info from database.

        Args:
            agent_id: Agent identifier

        Returns:
            WorktreeInfo object or None if not found
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT agent_id, path, branch, created_at, last_active, status
            FROM worktrees
            WHERE agent_id = ?
        """,
            (agent_id,),
        )

        row = cursor.fetchone()
        conn.close()

        if row:
            return WorktreeInfo(
                agent_id=row[0],
                path=Path(row[1]),
                branch=row[2],
                created_at=datetime.fromisoformat(row[3]),
                last_active=datetime.fromisoformat(row[4]),
                status=row[5],
            )
        return None

    def update_last_active(self, agent_id: str):
        """
        Update last_active timestamp for worktree.

        Args:
            agent_id: Agent identifier

        Example:
            ```python
            # Call this periodically to track active worktrees
            manager.update_last_active("agent-1")
            ```
        """
        now = datetime.now(timezone.utc).isoformat()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute(
            """
            UPDATE worktrees
            SET last_active = ?
            WHERE agent_id = ?
        """,
            (now, agent_id),
        )

        conn.commit()
        conn.close()

    def cleanup_stale_worktrees(self, stale_threshold_minutes: int = 60):
        """
        Remove stale worktrees (inactive for > threshold minutes).

        Args:
            stale_threshold_minutes: Minutes of inactivity before cleanup

        Returns:
            Number of worktrees cleaned up

        Example:
            ```python
            # Clean up worktrees inactive for > 60 minutes
            cleaned = manager.cleanup_stale_worktrees(60)
            print(f"Cleaned up {cleaned} stale worktrees")
            ```
        """
        now = datetime.now(timezone.utc)
        worktrees = self.list_worktrees()
        cleaned = 0

        for wt in worktrees:
            inactive_minutes = (now - wt.last_active).total_seconds() / 60
            if inactive_minutes > stale_threshold_minutes:
                logger.info(
                    f"Cleaning up stale worktree: {wt.agent_id} "
                    f"(inactive for {inactive_minutes:.1f} minutes)"
                )
                try:
                    self.remove_worktree(wt.agent_id, force=True)
                    cleaned += 1
                except Exception as e:
                    logger.error(f"Failed to cleanup worktree {wt.agent_id}: {e}")

        logger.info(f"Cleanup complete: {cleaned} stale worktrees removed")
        return cleaned


# ============================================================================
# Example Usage
# ============================================================================


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )

    # Example: Create and manage worktrees
    manager = GitWorktreeManager("/Users/jsncrabtree/PycharmProjects/FastAPIProject")

    # Create worktrees for 3 agents
    for i in range(1, 4):
        agent_id = f"agent-{i}"
        worktree_path = manager.create_worktree(agent_id)
        print(f"Created worktree: {worktree_path}")

    # List all worktrees
    worktrees = manager.list_worktrees()
    print(f"\nActive worktrees: {len(worktrees)}")
    for wt in worktrees:
        print(f"  {wt.agent_id}: {wt.path}")

    # Check for conflicts before merge
    conflicts = manager.detect_conflicts("agent-1")
    if conflicts:
        print(f"\nConflicts detected for agent-1:")
        for cf in conflicts:
            print(f"  {cf.path} ({cf.conflict_type})")
    else:
        print("\nNo conflicts, safe to merge agent-1")

    # Cleanup (for demo purposes)
    for i in range(1, 4):
        manager.remove_worktree(f"agent-{i}", force=True)
        print(f"Removed worktree: agent-{i}")
