# CDM Task Service Client Release Notes

# 0.2.3 (26/01/14)

* Update the `print_logs()` function to handle the case where the `stdout` stream doesn't
  have a `buffer` attribute.

# 0.2.2 (26/01/12)

* Added a method to cancel a job.
* Added a method to get a job's container exit codes.
* Added methods to stream a job container's logs.
* Added the declobber optional argument to the job submit method.
* Added a method to list available images.
* Added a method to list jobs.
* Added a method to get compute site status.
* Added a method to get user data, in particular what S3 paths are available for reading and
  writing data.
* Updated documentation for clarity

# 0.2.1 (25/10/27)

* Update the client to handle event importer updates for jobs where no importer is configured
  for the image.

## 0.2.0 (25/08/18)

* Removed the `InsertFiles` and `InsertContainerNumber` classes in favor of classmethods
  on the client.

## 0.1.0 (25/07/11)

* Initial release