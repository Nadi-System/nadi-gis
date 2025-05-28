import pathlib
from shutil import which
from qgis.core import QgsBlockingProcess
import platform


def nadi_bin_path():
    from_path = which("nadi-gis")
    if from_path is not None:
        return from_path
    nadi_bin = pathlib.Path(__file__).parent.joinpath("bin").joinpath(platform.system()).joinpath("nadi-gis").resolve()
    return str(nadi_bin.as_posix())

def qgis_nadi_proc(feedback, cmd):
    def stdout_handlr(bytes_array):
        lines = stdout_handlr._buffer + bytes_array.data().decode("utf-8")
        if not lines.endswith('\n'):
            try:
                lines, stdout_handlr._buffer = lines.rsplit('\n', maxsplit=1)
            except ValueError:
                stdout_handlr._buffer = lines
                return
        for line in lines.strip().split('\n'):
            try:
                label, progress = line.strip().split(":", maxsplit=1)
                if label != stdout_handlr._curr:
                    feedback.setProgressText(label)
                    stdout_handlr._curr = label
                feedback.setProgress(int(progress))
            except ValueError:
                feedback.pushInfo(line)

    def stderr_handlr(bytes_array):
        lines = stderr_handlr._buffer + bytes_array.data().decode("utf-8")
        if not lines.endswith('\n'):
            try:
                lines, stderr_handlr._buffer = lines.rsplit('\n', maxsplit=1)
            except ValueError:
                stderr_handlr._buffer = lines
                return
        for line in lines.strip().split('\n'):
            feedback.pushWarning(line)
    stdout_handlr._buffer = ''
    stderr_handlr._buffer = ''
    stdout_handlr._curr = ''

    nadi_bin = nadi_bin_path()
    proc = QgsBlockingProcess(nadi_bin, cmd)
    proc.setStdOutHandler(stdout_handlr)
    proc.setStdErrHandler(stderr_handlr)
    feedback.pushInfo("Running Nadi Command:")
    feedback.pushCommandInfo(nadi_bin)
    feedback.pushCommandInfo(" ".join(cmd))
    feedback.pushInfo("Output:")
    return proc
