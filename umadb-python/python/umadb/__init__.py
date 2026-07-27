from .umadb import *

# import atexit
import signal

# def _handle_exit_gracefully(*args, **kwargs):
#     """Triggered naturally when the main thread finishes or exits cleanly."""
#     try:
#         stop_all_stream_responses()  # Causes StopIteration in streaming responses.
#     except BaseException:
#         pass

def _handle_sigint_globally(signum, frame):
    """Triggered if the main thread catches Ctrl-C while idling."""
    try:
        cancel_all_stream_responses()  # Causes KeyboardInterrupt in streaming responses.
    except Exception:
        pass
    # Propagate the standard KeyboardInterrupt exception to the main thread
    raise KeyboardInterrupt

# TODO: This probably doesn't do anything useful...
# # Hook A: For normal exit (will trigger non-daemon thread)
# atexit.register(_handle_exit_gracefully)

# Hook B: Protects against Ctrl-C hangs when the main thread is idling or sleeping
# Python allows overriding this safely at module load time
signal.signal(signal.SIGINT, _handle_sigint_globally)
