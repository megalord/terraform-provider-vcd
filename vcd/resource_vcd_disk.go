package vcd

import (
	"fmt"
	"log"

	"github.com/hashicorp/terraform/helper/schema"
	"github.com/vmware/go-vcloud-director/govcd"
	types "github.com/vmware/go-vcloud-director/types/v56"
)

func resourceVcdDisk() *schema.Resource {
	return &schema.Resource{
		Create: resourceVcdDiskCreate,
		Delete: resourceVcdDiskDelete,
		Read:   resourceVcdDiskRead,
		Update: resourceVcdDiskUpdate,

		Schema: map[string]*schema.Schema{
			"org": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "Organization to create the disk in",
			},
			"vdc": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "VDC to create the disk in",
			},
			"name": &schema.Schema{
				Type:     schema.TypeString,
				Required: true,
				ForceNew: false,
			},
			"description": &schema.Schema{
				Type:     schema.TypeString,
				Optional: true,
				ForceNew: false,
			},
			"size": &schema.Schema{
				Type:     schema.TypeInt,
				Required: true,
				ForceNew: false,
			},

			"href": &schema.Schema{
				Type:     schema.TypeString,
				Optional: true,
				Computed: true,
			},
		},
	}
}

// Creates a new disk from a resource definition
func resourceVcdDiskCreate(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[TRACE] disk creation initiated")

	vcdClient := meta.(*VCDClient)

	_, vdc, err := vcdClient.GetOrgAndVdcFromResource(d)
	if err != nil {
		return fmt.Errorf("error retrieving Org and VDC: %s", err)
	}

	params, err := getDiskInput(d, vcdClient)
	if err != nil {
		return err
	}

	task, err := vdc.CreateDisk(params)
	if err != nil {
		log.Printf("[DEBUG] Error creating disk: %#v", err)
		return fmt.Errorf("error creating disk: %#v", err)
	}

	d.Set("href", task.Task.Owner.HREF)

	err = task.WaitTaskCompletion()
	if err != nil {
		log.Printf("[DEBUG] Error waiting for disk to finish: %#v", err)
		return fmt.Errorf("error waiting for disk to finish: %#v", err)
	}

	d.SetId(d.Get("name").(string))
	log.Printf("[TRACE] disk created: %#v", task)
	return nil
}

// Fetches information about an existing disk for a data definition
func resourceVcdDiskRead(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[TRACE] disk read initiated")

	vcdClient := meta.(*VCDClient)

	disk, err := govcd.FindDiskByHREF(&vcdClient.VCDClient.Client, d.Get("href").(string))
	if err != nil {
		log.Printf("[DEBUG] Unable to find disk. Removing from tfstate")
		d.SetId("")
		return nil
	}

	log.Printf("[TRACE] disk read completed: %#v", disk)
	return nil
}

func resourceVcdDiskUpdate(d *schema.ResourceData, m interface{}) error {
	return nil
}

// Deletes a disk, optionally removing all objects in it as well
func resourceVcdDiskDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[TRACE] disk delete started")

	vcdClient := meta.(*VCDClient)

	disk, err := govcd.FindDiskByHREF(&vcdClient.VCDClient.Client, d.Get("href").(string))
	if err != nil {
		log.Printf("[DEBUG] Unable to find disk. Removing from tfstate")
		d.SetId("")
		return nil
	}

	task, err := disk.Delete()
	if err != nil {
		log.Printf("[DEBUG] Error removing disk %#v", err)
		return fmt.Errorf("error removing disk %#v", err)
	}

	err = task.WaitTaskCompletion()
	if err != nil {
		log.Printf("[DEBUG] Error removing disk %#v", err)
		return fmt.Errorf("error removing disk %#v", err)
	}

	log.Printf("[TRACE] disk delete completed: %#v", disk)
	return nil
}

// helper for tranforming the resource input into the DiskCreateParams structure
// any cast operations or default values should be done here so that the create method is simple
func getDiskInput(d *schema.ResourceData, vcdClient *VCDClient) (*types.DiskCreateParams, error) {
	params := &types.DiskCreateParams{
		Disk: &types.Disk{
			Name: d.Get("name").(string),
			Size: d.Get("size").(int),
		},
	}

	if description, ok := d.GetOk("description"); ok {
		params.Disk.Description = description.(string)
	}

	return params, nil
}
