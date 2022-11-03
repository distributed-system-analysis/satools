satools
=======

*** Obsolete *** Use [PCP](https://github.com/performancecopilot/pcp) instead.

Tools for working with sar binary data files independent of the version
of sar that wrote them. It also includes a tool to translate XML output
to JSON, primarily useful in situations where versions of the sysstat
package don't support JSON output directly from the sadf command.

To use:

```
$ # From root of cloned tree
$ export PYTHONPATH=$(pwd)
$ cli/verifysa <file>
$ cli/extractsa <file>
$ cli/oscode <file>
$ cli/xmltojson <file>.xml <target-directory>
```

Released under the MIT License.
