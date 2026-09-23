UPDATE hosts
SET protocol = ?, port = ?, hypervisor = ?, cpu = ?, memory = ?, storage = ?
WHERE name = ? AND address = ?;
