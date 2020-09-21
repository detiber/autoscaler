/*
Copyright 2019 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package packet

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"path"
	"strings"
	"text/template"
	"time"

	"gopkg.in/gcfg.v1"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	klog "k8s.io/klog/v2"
	schedulerframework "k8s.io/kubernetes/pkg/scheduler/framework/v1alpha1"
)

type packetManagerRest struct {
	baseURL           string
	clusterName       string
	projectID         string
	apiServerEndpoint string
	facility          string
	plan              string
	os                string
	billing           string
	cloudinit         string
	reservation       string
	hostnamePattern   string
}

// ConfigGlobal options only include the project-id for now.
type ConfigGlobal struct {
	ClusterName       string `gcfg:"cluster-name"`
	ProjectID         string `gcfg:"project-id"`
	APIServerEndpoint string `gcfg:"api-server-endpoint"`
	Facility          string `gcfg:"facility"`
	Plan              string `gcfg:"plan"`
	OS                string `gcfg:"os"`
	Billing           string `gcfg:"billing"`
	CloudInit         string `gcfg:"cloudinit"`
	Reservation       string `gcfg:"reservation"`
	HostnamePattern   string `gcfg:"hostname-pattern"`
}

// ConfigFile is used to read and store information from the cloud configuration file.
type ConfigFile struct {
	Global ConfigGlobal `gcfg:"global"`
}

// Device represents a Packet device.
type Device struct {
	ID          string   `json:"id"`
	ShortID     string   `json:"short_id"`
	Hostname    string   `json:"hostname"`
	Description string   `json:"description"`
	State       string   `json:"state"`
	Tags        []string `json:"tags"`
}

// Devices represents a list of Packet devices.
type Devices struct {
	Devices []Device `json:"devices"`
}

// IPAddressCreateRequest represents a request to create a new IP address within a DeviceCreateRequest.
type IPAddressCreateRequest struct {
	AddressFamily int  `json:"address_family"`
	Public        bool `json:"public"`
}

// DeviceCreateRequest represents a request to create a new Packet device. Used by createNodes.
type DeviceCreateRequest struct {
	Hostname              string                   `json:"hostname"`
	Plan                  string                   `json:"plan"`
	Facility              []string                 `json:"facility"`
	OS                    string                   `json:"operating_system"`
	BillingCycle          string                   `json:"billing_cycle"`
	ProjectID             string                   `json:"project_id"`
	UserData              string                   `json:"userdata"`
	Storage               string                   `json:"storage,omitempty"`
	Tags                  []string                 `json:"tags"`
	CustomData            string                   `json:"customdata,omitempty"`
	IPAddresses           []IPAddressCreateRequest `json:"ip_addresses,omitempty"`
	HardwareReservationID string                   `json:"hardware_reservation_id,omitempty"`
}

// CloudInitTemplateData represents the variables that can be used in cloudinit templates.
type CloudInitTemplateData struct {
	BootstrapTokenID     string
	BootstrapTokenSecret string
	APIServerEndpoint    string
}

// HostnameTemplateData represents the template variables used to construct host names for new nodes.
type HostnameTemplateData struct {
	ClusterName string
	NodeGroup   string
	RandString8 string
}

var (
	ErrUnkown       = fmt.Errorf("unknown error")
	ErrCreateDevice = fmt.Errorf("failed to create device")
)

// Contains tells whether a contains x.
func Contains(a []string, x string) bool {
	for _, n := range a {
		if x == n {
			return true
		}
	}

	return false
}

// createPacketManagerRest sets up the client and returns
// an packetManagerRest.
func createPacketManagerRest(configReader io.Reader, opts config.AutoscalingOptions) (*packetManagerRest, error) {
	var cfg ConfigFile

	if configReader != nil {
		if err := gcfg.ReadInto(&cfg, configReader); err != nil {
			klog.Errorf("Couldn't read config: %v", err)

			return nil, err
		}
	}

	if opts.ClusterName == "" && cfg.Global.ClusterName == "" {
		klog.Fatalf("The cluster-name parameter must be set")
	}

	if opts.ClusterName != "" && cfg.Global.ClusterName == "" {
		cfg.Global.ClusterName = opts.ClusterName
	}

	manager := packetManagerRest{
		baseURL:           "https://api.packet.net",
		clusterName:       cfg.Global.ClusterName,
		projectID:         cfg.Global.ProjectID,
		apiServerEndpoint: cfg.Global.APIServerEndpoint,
		facility:          cfg.Global.Facility,
		plan:              cfg.Global.Plan,
		os:                cfg.Global.OS,
		billing:           cfg.Global.Billing,
		cloudinit:         cfg.Global.CloudInit,
		reservation:       cfg.Global.Reservation,
		hostnamePattern:   cfg.Global.HostnamePattern,
	}

	return &manager, nil
}

func (mgr *packetManagerRest) listPacketDevices(ctx context.Context) (*Devices, error) {
	jsonStr := []byte(``)
	packetAuthToken := os.Getenv("PACKET_AUTH_TOKEN")
	url := mgr.baseURL + "/projects/" + mgr.projectID + "/devices"

	req, err := http.NewRequestWithContext(ctx, "GET", url, bytes.NewBuffer(jsonStr))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("X-Auth-Token", packetAuthToken)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to perform request: %w", err)
	}

	defer func() {
		if err := resp.Body.Close(); err != nil {
			klog.Errorf("failed to close response body: %v", err)
		}
	}()

	klog.Infof("response Status: %s", resp.Status)

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("bad response: %w", err)
	}

	var devices Devices
	if err := json.Unmarshal(body, &devices); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response body: %w", err)
	}

	return &devices, nil
}

// nodeGroupSize gets the current size of the nodegroup as reported by packet tags.
func (mgr *packetManagerRest) nodeGroupSize(ctx context.Context, nodegroup string) (int, error) {
	devices, err := mgr.listPacketDevices(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to list devices: %w", err)
	}

	// Get the count of devices tagged as nodegroup members.
	count := 0

	for _, d := range devices.Devices {
		if Contains(d.Tags, "k8s-cluster-"+mgr.clusterName) && Contains(d.Tags, "k8s-nodepool-"+nodegroup) {
			count++
		}
	}

	klog.V(3).Infof("Nodegroup %s: %d/%d", nodegroup, count, len(devices.Devices))

	return count, nil
}

func randString8() string {
	n := 8

	rand.Seed(time.Now().UnixNano())

	letterRunes := []rune("acdefghijklmnopqrstuvwxyz")

	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}

	return string(b)
}

func (mgr *packetManagerRest) createNode(ctx context.Context, cloudinit, nodegroup string) error {
	udvars := CloudInitTemplateData{
		BootstrapTokenID:     os.Getenv("BOOTSTRAP_TOKEN_ID"),
		BootstrapTokenSecret: os.Getenv("BOOTSTRAP_TOKEN_SECRET"),
		APIServerEndpoint:    mgr.apiServerEndpoint,
	}

	ud, err := renderTemplate(cloudinit, udvars)
	if err != nil {
		return fmt.Errorf("failed to create userdata from template: %w", err)
	}

	hnvars := HostnameTemplateData{
		ClusterName: mgr.clusterName,
		NodeGroup:   nodegroup,
		RandString8: randString8(),
	}

	hn, err := renderTemplate(mgr.hostnamePattern, hnvars)
	if err != nil {
		return fmt.Errorf("failed to create hostname from template: %w", err)
	}

	if err := mgr.createDevice(ctx, hn, ud, nodegroup); err != nil {
		return fmt.Errorf("failed to create device %q in node group %q: %w", hn, nodegroup, err)
	}

	klog.Infof("Created new node on Packet.")

	return nil
}

// createNodes provisions new nodes on packet and bootstraps them in the cluster.
func (mgr *packetManagerRest) createNodes(ctx context.Context, nodegroup string, nodes int) error {
	klog.Infof("Updating node count to %d for nodegroup %s", nodes, nodegroup)

	cloudinit, err := base64.StdEncoding.DecodeString(mgr.cloudinit)
	if err != nil {
		log.Fatal(err)

		return fmt.Errorf("could not decode cloudinit script: %w", err)
	}

	errList := make([]error, 0, nodes)
	for i := 0; i < nodes; i++ {
		errList = append(errList, mgr.createNode(ctx, string(cloudinit), nodegroup))
	}

	return utilerrors.NewAggregate(errList)
}

func (mgr *packetManagerRest) createDevice(ctx context.Context, hostname, userData, nodeGroup string) error {
	reservation := ""
	if mgr.reservation == "require" || mgr.reservation == "prefer" {
		reservation = "next-available"
	}

	cr := &DeviceCreateRequest{
		Hostname:              hostname,
		Facility:              []string{mgr.facility},
		Plan:                  mgr.plan,
		OS:                    mgr.os,
		ProjectID:             mgr.projectID,
		BillingCycle:          mgr.billing,
		UserData:              userData,
		Tags:                  []string{"k8s-cluster-" + mgr.clusterName, "k8s-nodepool-" + nodeGroup},
		HardwareReservationID: reservation,
	}

	if err := createDevice(ctx, cr, mgr.baseURL); err != nil {
		if reservation != "" && mgr.reservation == "prefer" && isNoAvailableReservationsError(err) {
			klog.Infof("Reservation preferred but not available. Provisioning on-demand node.")

			cr.HardwareReservationID = ""

			return createDevice(ctx, cr, mgr.baseURL)
		}

		return err
	}

	return nil
}

// TODO: find a better way than parsing the error messages for this.
func isNoAvailableReservationsError(err error) bool {
	return strings.Contains(err.Error(), " no available hardware reservations ")
}

func createDevice(ctx context.Context, cr *DeviceCreateRequest, baseURL string) error {
	packetAuthToken := os.Getenv("PACKET_AUTH_TOKEN")
	url := baseURL + "/projects/" + cr.ProjectID + "/devices"

	jsonValue, err := json.Marshal(cr)
	if err != nil {
		return fmt.Errorf("failed to marshal create request: %w", err)
	}

	klog.Infof("Creating new node")
	klog.V(3).Infof("POST %s \n%v", url, string(jsonValue))

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonValue))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("X-Auth-Token", packetAuthToken)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to perform request: %w", err)
	}

	defer func() {
		if err := resp.Body.Close(); err != nil {
			klog.Errorf("failed to close response body: %v", err)
		}
	}()

	rbody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}

	klog.V(3).Infof("Response body: %v", string(rbody))

	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
		return nil
	}

	var result interface{}
	if err := json.Unmarshal(rbody, &result); err != nil {
		return fmt.Errorf("request failed: %v, failed to unmarshal the response: %v, error: %w", resp, result, err)
	}

	mappedResult := result.(map[string]interface{})
	if errorMessages, ok := mappedResult["errors"]; ok {
		return fmt.Errorf("result: %v, error: %w", errorMessages, ErrCreateDevice)
	}

	return fmt.Errorf("request failed, result: %v, error: %w", result, ErrUnkown)
}

// getNodes should return ProviderIDs for all nodes in the node group,
// used to find any nodes which are unregistered in kubernetes.
func (mgr *packetManagerRest) getNodes(ctx context.Context, nodegroup string) ([]string, error) {
	// Get node ProviderIDs by getting device IDs from Packet
	devices, err := mgr.listPacketDevices(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list devices: %w", err)
	}

	nodes := []string{}

	for _, d := range devices.Devices {
		if Contains(d.Tags, "k8s-cluster-"+mgr.clusterName) && Contains(d.Tags, "k8s-nodepool-"+nodegroup) {
			nodes = append(nodes, d.ID)
		}
	}

	return nodes, nil
}

// getNodeNames should return Names for all nodes in the node group,
// used to find any nodes which are unregistered in kubernetes.
func (mgr *packetManagerRest) getNodeNames(ctx context.Context, nodegroup string) ([]string, error) {
	devices, err := mgr.listPacketDevices(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list devices: %w", err)
	}

	nodes := []string{}

	for _, d := range devices.Devices {
		if Contains(d.Tags, "k8s-cluster-"+mgr.clusterName) && Contains(d.Tags, "k8s-nodepool-"+nodegroup) {
			nodes = append(nodes, d.Hostname)
		}
	}

	return nodes, nil
}

// deleteNodes deletes nodes by passing a comma separated list of names or IPs.
func (mgr *packetManagerRest) deleteNodes(ctx context.Context, nodegroup string, nodes []NodeRef, updatedNodeCount int) error {
	klog.Infof("Deleting nodes %v", nodes)

	for _, n := range nodes {
		klog.Infof("Node %s - %s - %s", n.Name, n.MachineID, n.IPs)

		if err := mgr.deleteNode(ctx, nodegroup, n); err != nil {
			return err
		}
	}

	return nil
}

func (mgr *packetManagerRest) deleteNode(ctx context.Context, nodeGroup string, n NodeRef) error {
	dl, err := mgr.listPacketDevices(ctx)
	if err != nil {
		return fmt.Errorf("failed to list devices: %w", err)
	}

	klog.Infof("%d devices total", len(dl.Devices))
	// Get the count of devices tagged as nodegroup
	for _, d := range dl.Devices {
		klog.Infof("Checking device %v", d)

		if err := mgr.deleteDevice(ctx, nodeGroup, n.Name, d); err != nil {
			return fmt.Errorf("failed to delete node: %w", err)
		}
	}

	return nil
}

func (mgr *packetManagerRest) deleteDevice(ctx context.Context, nodeGroup, nodeName string, d Device) error {
	if Contains(d.Tags, "k8s-cluster-"+mgr.clusterName) && Contains(d.Tags, "k8s-nodepool-"+nodeGroup) {
		klog.Infof("nodegroup match %s %s", d.Hostname, nodeName)

		if d.Hostname == nodeName {
			klog.V(1).Infof("Matching Packet Device %s - %s", d.Hostname, d.ID)

			if err := deleteDevice(ctx, d.ID, mgr.baseURL); err != nil {
				return fmt.Errorf("failed to delete device: %w", err)
			}
		}
	}

	return nil
}

func deleteDevice(ctx context.Context, id, baseURL string) error {
	packetAuthToken := os.Getenv("PACKET_AUTH_TOKEN")
	url := path.Join(baseURL + "devices" + id)

	req, err := http.NewRequestWithContext(ctx, "DELETE", url, bytes.NewBuffer([]byte("")))
	if err != nil {
		return fmt.Errorf("failed to delete device %q: %w", id, err)
	}

	req.Header.Set("X-Auth-Token", packetAuthToken)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to delete device: %w", err)
	}

	defer func() {
		if err := resp.Body.Close(); err != nil {
			klog.Errorf("failed to close response body: %v", err)
		}
	}()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response: %v error: %w", resp, err)
	}

	klog.Infof("Deleted device %s: %v", id, body)

	return nil
}

// templateNodeInfo returns a NodeInfo with a node template based on the VM flavor
// that is used to created minions in a given node group.
func (mgr *packetManagerRest) templateNodeInfo(nodegroup string) (*schedulerframework.NodeInfo, error) {
	return nil, cloudprovider.ErrNotImplemented
}

func renderTemplate(str string, vars interface{}) (string, error) {
	tmpl, err := template.New("tmpl").Parse(str)
	if err != nil {
		return "", fmt.Errorf("failed to parse template %q, %w", str, err)
	}

	var tmplBytes bytes.Buffer

	if err := tmpl.Execute(&tmplBytes, vars); err != nil {
		return "", fmt.Errorf("failed to execute template: %w", err)
	}

	return tmplBytes.String(), nil
}
