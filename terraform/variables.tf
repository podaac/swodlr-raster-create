variable "app_name" {
    default = "swodlr"
    type = string
}

variable "service_name" {
    default = "raster-create"
    type = string
}

variable "default_tags" {
    type = map(string)
    default = {}
}

variable "stage" {
    type = string
}

variable "region" {
    type = string
}

variable "sds_pcm_release_tag" {
    type = string
}

variable "sds_host" {
    type = string
    sensitive = true
}

variable "sds_username" {
    type = string
    sensitive = true
}

variable "sds_password" {
    type = string
    sensitive = true
}

variable "sds_ca_cert_path" {
    type = string
    default = "/etc/ssl/certs/JPLICA.Root.pem"
}

variable "sds_submit_max_attempts" {
    type = number
    default = 5
}

variable "sds_submit_timeout" {
    type = number
    default = 20
}

variable "update_max_attempts" {
    type = number
    default = 5
}
