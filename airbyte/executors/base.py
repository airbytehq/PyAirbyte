# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
from __future__ import annotations

import subprocess
from abc import ABC, abstractmethod
from contextlib import contextmanager
from pathlib import Path
from threading import Thread
from typing import IO, TYPE_CHECKING, Any, cast

import ulid
from rich import print

from airbyte import exceptions as exc
from airbyte._message_generators import AirbyteMessageGenerator  # noqa: PLC2701
from airbyte.sources.registry import ConnectorMetadata


if TYPE_CHECKING:
    from collections.abc import Generator, Iterable, Iterator


_LATEST_VERSION = "latest"


def _pump_input(pipe: IO[str], messages: AirbyteMessageGenerator) -> None:
    """Pump lines into a pipe."""
    with pipe:
        pipe.writelines(message.model_dump_json() + "\n" for message in messages)
        pipe.flush()  # Ensure data is sent immediately


def _stream_from_file(file: IO[str]) -> Generator[str, Any, None]:
    """Stream lines from a file."""
    while True:
        line = file.readline()
        if not line:
            break
        yield line


@contextmanager
def _stream_from_subprocess(
    args: list[str],
    *,
    stdin: IO[str] | AirbyteMessageGenerator | None = None,
    log_file: IO[str] | None = None,
) -> Generator[Iterable[str], None, None]:
    """Stream lines from a subprocess."""
    input_thread: Thread | None = None
    Path.mkdir(Path.cwd() / "logs", exist_ok=True)
    process_log_file = Path(f"./logs/process_log_{ulid.ULID()!s}.log")
    print(f"Logging process output to {process_log_file!s}")
    if isinstance(stdin, AirbyteMessageGenerator):
        process = subprocess.Popen(
            args,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=log_file,
            universal_newlines=True,
            encoding="utf-8",
        )
        input_thread = Thread(target=_pump_input, args=(process.stdin, stdin))
        input_thread.start()
        input_thread.join()  # Ensure the input thread has finished
    else:
        # stdin is None or a file-like object
        process = subprocess.Popen(
            args,
            stdin=stdin,
            stdout=subprocess.PIPE,
            stderr=log_file,
            universal_newlines=True,
            encoding="utf-8",
        )

    if process.stdout is None:
        raise exc.AirbyteSubprocessError(
            message="Subprocess did not return a stdout stream.",
            context={
                "args": args,
                "returncode": process.returncode,
            },
        )

    try:
        yield _stream_from_file(process.stdout)
        process.wait()
    finally:
        # Close the stdout stream
        if process.stdout:
            process.stdout.close()

        # Terminate the process if it is still running
        if process.poll() is None:  # Check if the process is still running
            process.terminate()
            try:
                # Wait for a short period to allow process to terminate gracefully
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                # If the process does not terminate within the timeout, force kill it
                process.kill()

        # Now, the process is either terminated or killed. Check the exit code.
        exit_code = process.wait()

        # If the exit code is not 0 or -15 (SIGTERM), raise an exception
        if exit_code not in {0, -15}:
            raise exc.AirbyteSubprocessFailedError(
                run_args=args,
                exit_code=exit_code,
            )


class Executor(ABC):
    def __init__(
        self,
        *,
        name: str | None = None,
        metadata: ConnectorMetadata | None = None,
        target_version: str | None = None,
    ) -> None:
        """Initialize a connector executor.

        The 'name' param is required if 'metadata' is None.
        """
        if not name and not metadata:
            raise exc.PyAirbyteInternalError(message="Either name or metadata must be provided.")

        self.name: str = name or cast(ConnectorMetadata, metadata).name  # metadata is not None here
        self.metadata: ConnectorMetadata | None = metadata
        self.enforce_version: bool = target_version is not None

        self.reported_version: str | None = None
        self.target_version: str | None = None
        if target_version:
            if metadata and target_version == _LATEST_VERSION:
                self.target_version = metadata.latest_available_version
            else:
                self.target_version = target_version

    @property
    @abstractmethod
    def _cli(self) -> list[str]:
        """Get the base args of the CLI executable.

        Args will be appended to this list.
        """
        ...

    def execute(
        self,
        args: list[str],
        *,
        stdin: IO[str] | AirbyteMessageGenerator | None = None,
    ) -> Iterator[str]:
        """Execute a command and return an iterator of STDOUT lines.

        If stdin is provided, it will be passed to the subprocess as STDIN.
        """
        with _stream_from_subprocess(
            [*self._cli, *args],
            stdin=stdin,
        ) as stream_lines:
            yield from stream_lines

    @abstractmethod
    def ensure_installation(self, *, auto_fix: bool = True) -> None:
        _ = auto_fix
        pass

    @abstractmethod
    def install(self) -> None:
        pass

    @abstractmethod
    def uninstall(self) -> None:
        pass

    def get_installed_version(
        self,
        *,
        raise_on_error: bool = False,
        recheck: bool = False,
    ) -> str | None:
        """Detect the version of the connector installed."""
        _ = raise_on_error, recheck  # Unused
        raise NotImplementedError(
            f"'{type(self).__name__}' class cannot yet detect connector versions."
        )