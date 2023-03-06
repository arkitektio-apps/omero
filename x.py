import os
import javabridge
import bioformats

javabridge.start_vm(run_headless=True, class_path=bioformats.JARS)
try:
    print(javabridge.run_script('java.lang.String.format("Hello, %s!", greetee);',
                                dict(greetee='world')))
finally:
    javabridge.kill_vm()
