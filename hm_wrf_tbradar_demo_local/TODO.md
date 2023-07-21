Luke Logan: In BufferOrganizer::GlobalOrganizeBlob, the first one in the file, add the following line of code after bkt.GetBlobId:
if (blob_id.IsNull()) {
return;
}

Luke Logan: This will prevent the fatal error from happening for every single thing

Luke Logan: This seems to be an issue with the behavior of the workload

